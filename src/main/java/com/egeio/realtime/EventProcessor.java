package com.egeio.realtime;

import com.egeio.core.config.Config;
import com.egeio.core.log.Logger;
import com.egeio.core.log.LoggerFactory;
import com.egeio.core.log.MyUUID;
import com.egeio.core.monitor.MonitorClient;
import com.egeio.core.utils.GsonUtils;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.*;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class is responsible for fetching the info from rabbitmq
 * and send http request to the real-time server
 * <p>
 * Created by think on 2015/8/2.
 */
public class EventProcessor implements Runnable {
    //queue name
    private static Logger logger = LoggerFactory
            .getLogger(EventProcessor.class);
    private static MyUUID uuid = new MyUUID();
    private static final String TASK_QUEUE_NAME = "new_message_queue";
    private static MemcachedClient memClient;
    private final static String memHost = Config.getConfig()
            .getElement("/configuration/memcached/host").getText();
    private final static long memPort = Config
            .getNumber("/configuration/memcached/port", 11211);

    private static String rabbitMqHost = Config.getConfig()
            .getElement("/configuration/rabbitmq/mq_host").getText();

    private static String hostName = Config.getConfig()
            .getElement("/configuration/ip_address").getText();
    private static String realtimeProtocol = Config.getConfig()
            .getElement("/configuration/realtime/protocol").getText();
    private static String realtimeURI = Config.getConfig()
            .getElement("/configuration/realtime/uri").getText();

    private QueueingConsumer consumer = null;
    private Channel channel = null;
    private ConnectionFactory factory = new ConnectionFactory();

    //monitoring thread
    private static long notificationNum = 0;
    private static MonitorClient opentsdbClient;
    private static ScheduledExecutorService monitorExecutor = Executors
            .newSingleThreadScheduledExecutor();

    //    initialization for Mem client
    static {
        try {
            memClient = new MemcachedClient(
                    AddrUtil.getAddresses(memHost + ":" + memPort));
        }
        catch (IOException e) {
            logger.error(uuid, "Init MemCached Client failed");
            System.exit(-1);
        }
        String metricPath = "/configuration/monitor/metric";
        String intervalPath = "/configuration/monitor/interval";
        opentsdbClient = MonitorClient.getInstance(
                Config.getConfig().getElement(metricPath).getText());

        long monitorInterval = Config.getNumber(intervalPath, 601);

        monitorExecutor.scheduleAtFixedRate(new Runnable() {
            @Override public void run() {
                sendMonitorInfo();
            }
        }, monitorInterval, monitorInterval, TimeUnit.SECONDS);
    }

    public static void sendMonitorInfo() {
        Map<String, String> tags = new HashMap<>();
        tags.put("type", "notification_num");
        try {
            tags.put("host", InetAddress.getLocalHost().getHostName());
        }
        catch (UnknownHostException e) {
            logger.error(uuid, "unknow host error!", e);
        }

        MonitorClient.Record record = new MonitorClient.Record(notificationNum,
                tags);
        List<MonitorClient.Record> records = new ArrayList<>();
        records.add(record);
        opentsdbClient.send(records);
        //reset
        resetNotificationNum();
    }

    private void connectMq() throws IOException, TimeoutException {
        Connection connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        /**
         * This tells RabbitMQ not to give more than one message to a
         * worker at a time. Or, in other words, don't dispatch a new
         * message to a worker until it has processed and acknowledged
         * the previous one. Instead, it will dispatch it to the next
         * worker that is not still busy
         */
        channel.basicQos(1);

        consumer = new QueueingConsumer(channel);
        //Use hostname as consume tagName, so that We can monitor who consume this Queue
        channel.basicConsume(TASK_QUEUE_NAME, false, hostName, consumer);
        logger.info(uuid, "Connected to rabbitmq server:{}", rabbitMqHost);
    }

    public void run() {
        factory.setHost(rabbitMqHost);
        String message;
        QueueingConsumer.Delivery delivery = null;
        JsonObject json;

        try {
            connectMq();
        }
        catch (Exception e) {
            logger.error(uuid, "Failed to connect to rabbit mq");
        }

        while (true) {
            try {
                delivery = consumer.nextDelivery();
            }
            catch (ShutdownSignalException e) {
                logger.error(uuid, "rabbitmq server has closed");
                break;
            }
            catch (ConsumerCancelledException e) {
                logger.error(uuid,
                        "The consumer has cancelled , Try to re-consume");
                //Sleep 1s and reconnect to rabbitmq-server
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e1) {
                    logger.error(uuid, "Thread interrupted");
                }

                try {
                    connectMq();
                    continue;
                }
                catch (Exception e1) {
                    logger.error(uuid, "Failed to connect to rabbit mq");
                }

            }
            catch (InterruptedException e) {
                logger.error(uuid, "Thread interrupted");
            }
            catch (NullPointerException e) {
                logger.error(uuid,
                        "No consumer created, try to connect to server again");
                //Sleep 1s and reconnect to rabbitmq-server
                break;
            }

            if (delivery == null) {
                logger.info(uuid, "Empty delivery from queue");
                continue;
            }
            try {
                Gson gson = GsonUtils.getGson();
                message = new String(delivery.getBody(), "UTF-8");
                json = gson.fromJson(message, JsonObject.class);
                if (json == null || json.get("user_id") == null) {
                    logger.error(uuid, "No user_id found in json object");
                }
                else {
                    logger.info(uuid, "Message:{}", message);
                    JsonArray userIDInfo = json.get("user_id").getAsJsonArray();
                    if (userIDInfo == null) {
                        logger.error(uuid, "Can't find userId in this message");
                        continue;
                    }
                    json.remove("user_id");
                    Map<String, List<String>> serverUserMap = getServerUserMap(
                            userIDInfo);
                    for (String address : serverUserMap.keySet()) {
                        List<String> userIdList = serverUserMap.get(address);
                        JsonElement userIDJson = gson
                                .fromJson(gson.toJson(userIdList),
                                        JsonArray.class);
                        JsonObject content = json.getAsJsonObject();
                        content.add("user_id", userIDJson);
                        sendNotification(realtimeProtocol, address, realtimeURI,
                                gson.toJson(content));

                        //add notification number
                        addNotificationNum(userIdList.size());
                    }

                }
            }
            catch (UnsupportedEncodingException e) {
                logger.error(uuid, "Unsupported Encoding Exception");
            }
            catch (Exception e) {
                logger.error(uuid, "GsonUtils error");
                e.printStackTrace();
            }
            finally {
                //send ack anyway
                try {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(),
                            false);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        //If because of the rabbitmq-server stop ,We will re-try connect to rabbitmq-server after 60s
        logger.warn(uuid,
                "The rabbitmq Server have broken , We Try to re-connect again After 10 seconds");
        try {
            Thread.sleep(1000 * 10);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.run();
    }

    private void sendNotification(String protocol, String host, String uri,
            String content) {
        String realTimeAddress = String
                .format("%s://%s%s", protocol, host, uri);
        HttpRequest.sendPost(realTimeAddress, "data=" + content);
        logger.info(uuid, "Try to send content:{} to {}", content,
                realTimeAddress);
    }

    private synchronized void addNotificationNum(int num) {
        notificationNum += num;
    }

    private static synchronized void resetNotificationNum() {
        notificationNum = 0;
    }

    /**
     * @param userIDs userIDs to be rearranged according to the address of real-time server
     * @return map which key is the address of real-time server and value is list of uids which currently connecting to the server
     */
    private Map<String, List<String>> getServerUserMap(JsonArray userIDs) {
        Map<String, List<String>> serverUserMap = new HashMap<>();
        for (JsonElement userID : userIDs) {
            if (!userID.isJsonNull() && !userID.getAsString().equals("")) {
                String uid = userID.getAsString();
                Set<String> addresses = getNodeAddressByUserID(uid);
                if (addresses != null) {
                    for (String address : addresses) {
                        if (serverUserMap.get(address) != null) {
                            serverUserMap.get(address).add(uid);
                        }
                        else {
                            List<String> list = new ArrayList<>();
                            list.add(uid);
                            serverUserMap.put(address, list);
                        }
                    }
                }
            }
        }
        return serverUserMap;
    }

    /**
     * Fetch the set of server addresses the user is currently connecting to
     * return null if the user is offline
     *
     * @param userID user id
     * @return The set of Server addresses for user
     */
    private Set<String> getNodeAddressByUserID(String userID) {
        Set<String> result = null;
        if (memClient.get(userID) == null) {
            result = null;
        }
        else {
            String jsonObj;
            try {
                jsonObj = GsonUtils.getGson().toJson(memClient.get(userID));
                result = GsonUtils.getGson().fromJson(
                        jsonObj.substring(1, jsonObj.length() - 1)
                                .replace("\\", ""),
                        new TypeToken<Set<String>>() {
                        }.getType());
            }
            catch (Exception e) {
                logger.error(uuid, "Json format exception");
            }

        }
        return result;
    }

    public static void main(String[] args) {
        logger.info(uuid, "Event processor starts");
        new EventProcessor().run();
    }

}
