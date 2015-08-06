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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.util.Set;

/**
 * This class is responsible for fetching the info from rabbitmq
 * and send http request to the real-time server
 * <p>
 * Created by think on 2015/8/2.
 */
public class EventProcessor {
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
    //set rabbitmq host to localhost for now

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

    public static void main(String[] argv) {
        ConnectionFactory factory = new ConnectionFactory();
        rabbitMqHost = "localhost";
        factory.setHost(rabbitMqHost);
        QueueingConsumer consumer = null;
        Channel channel = null;
        try {
            Connection connection = factory.newConnection();
            channel = connection.createChannel();

            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
            //dispatch tasks in balance
            channel.basicQos(1);

            consumer = new QueueingConsumer(channel);
            channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
        }
        catch (Exception e) {
            logger.error(uuid, "Failed to connect to rabbit mq");
            System.exit(-1);
        }

        //monitoring the rabbitmq
        String message = null;
        QueueingConsumer.Delivery delivery = null;
        while (true) {
            try {
                delivery = consumer.nextDelivery();
                message = new String(delivery.getBody(), "UTF-8");
            }
            catch (Exception e) {
                logger.error(uuid, "Message delivery failed");
            }

            JsonObject json = null;
            try {
                json = GsonUtils.getGson().fromJson(message, JsonObject.class);
            }
            catch (Exception e) {
                logger.info(uuid, "Json format error");
            }

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
            try {
                if (delivery != null) {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(),
                            false);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

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

}
