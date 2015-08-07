package com.egeio.realtime;

import com.egeio.core.config.Config;
import com.egeio.core.log.Logger;
import com.egeio.core.log.LoggerFactory;
import com.egeio.core.log.MyUUID;
import com.egeio.core.utils.GsonUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.*;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Set;
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

//    private static String rabbitMqHost = Config.getConfig()
//            .getElement("/configuration/rabbitmq/mq_host").getText();
    //set rabbitmq host to localhost for now
    private static String hostName = Config.getConfig()
            .getElement("/configuration/ip_address").getText();

    private QueueingConsumer consumer = null;
    private Channel channel = null;
    private Connection connection = null;
    private ConnectionFactory factory = new ConnectionFactory();

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
    }

    public void connectMq() throws IOException, TimeoutException {
        connection = factory.newConnection();
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
        //Use hostname as consume tagName , So that We can monitor who consume this Queue
        channel.basicConsume(TASK_QUEUE_NAME, false, hostName, consumer);
    }

    public void disconnectMq() throws IOException, TimeoutException {
        channel.basicCancel(hostName);
        channel.close();
        connection.close();
    }

    public void run() {

        String rabbitMqHost = "localhost";
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

            try {
                if (delivery == null) {
                    logger.info(uuid, "Error delivery from queue");
                    continue;
                }
                message = new String(delivery.getBody(), "UTF-8");
                json = GsonUtils.getGson().fromJson(message, JsonObject.class);
                if (json == null || json.get("user_id") == null) {
                    logger.error(uuid, "No user_id found in json object");
                }
                else {
                    JsonArray userIDInfo = json.get("user_id").getAsJsonArray();
                    logger.info(uuid, "Received userID to push:{}", userIDInfo);
                    for (JsonElement userID : userIDInfo) {
                        handleMessage(userID.getAsString());
                    }
                    logger.info(uuid, "Message:{}", message);
                }
            }
            catch (UnsupportedEncodingException e) {
                logger.error(uuid, "Unsupported Encoding Exception");
                try {
                    disconnectMq();
                }
                catch (Exception e1) {
                    logger.error(uuid,
                            "Failed to close connection and channel");
                }
                continue;
            }
            catch (Exception e) {
                logger.error(uuid, "GsonUtils error");
                try {
                    disconnectMq();
                }
                catch (Exception e1) {
                    logger.error(uuid,
                            "Failed to close connection and channel");
                }
                continue;
            }

            /**
             * If a consumer dies without sending an ack, RabbitMQ will
             * understand that a message wasn't processed fully and will
             * redeliver it to another consumer
             * There aren't any message timeouts;
             * RabbitMQ will redeliver the message only when
             * the worker connection dies. It's fine even if processing
             * a message takes a very, very long time
             */

            try {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(),
                        false);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        //If because of the rabbitmq-server stop ,We will re-try connect to rabbitmq-server after 60s
        logger.warn(uuid,
                "The rabbitmq Server have broken , We Try to re-connect again After 60 seconds");
        try {
            Thread.sleep(1000 * 60);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.run();
    }

    /**
     * notify user if the user is online
     *
     * @param userID user id
     */
    private static void handleMessage(String userID) {
        //Get all real-time server nodes the user is connecting
        Set<String> addresses = getNodeAddressByUserID(userID);
        if (addresses == null) {
            logger.info(uuid, "user {} is not online", userID);
            return;
        }
        for (String address : addresses) {
            String realTimeNode = "http://" + address + "/push";
            logger.info(uuid, "Real-time Node address:{} for user:{}",
                    realTimeNode, userID);
            HttpRequest.sendPost(realTimeNode, "userID=" + userID);
        }
    }

    /**
     * Fetch the set of server addresses the user is currently connecting to
     * return null if the user is offline
     *
     * @param userID user id
     * @return The set of Server addresses for user
     */
    private static Set<String> getNodeAddressByUserID(String userID) {
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

    public static void main(String[] args){
        logger.info(uuid,"Event processor starts");
        new EventProcessor().run();
    }

}
