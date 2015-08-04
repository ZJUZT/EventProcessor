package com.egeio.realtime;

import com.egeio.core.config.Config;
import com.egeio.core.log.Logger;
import com.egeio.core.log.LoggerFactory;
import com.egeio.core.log.MyUUID;
import com.egeio.core.utils.GsonUtils;
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
    private static final String URL = "";
    private static MemcachedClient memClient;
    private final static String memHost = Config.getConfig()
            .getElement("/configuration/memcached/host").getText();
    private final static long memPort = Config
            .getNumber("/configuration/memcached/port", 11211);

//    private final static String rabbitMqHost = Config.getConfig()
//            .getElement("/configuration/rabbitmq/mq_host").getText();
    //set rabbitmq host to localhost for now


    static {
        try {
            memClient = new MemcachedClient(
                    AddrUtil.getAddresses(memHost + ":" + memPort));
        }
        catch (IOException e) {
            logger.error(uuid, "Init MemCached Client failed");
        }
    }

    public static void main(String[] argv) throws Exception {

        String rabbitMqHost = "localhost";
//        String rabbitMqHost = Config.getConfig()
//                    .getElement("/configuration/rabbitmq/mq_host").getText();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMqHost);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        //dispatch tasks in balance
        channel.basicQos(1);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(TASK_QUEUE_NAME, false, consumer);

        //monitoring the rabbitmq
        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String userID = new String(delivery.getBody(), "UTF-8");

            System.out.println(" [x] Received '" + userID + "'");
            //            HttpRequest s = new HttpRequest("localhost",8081);
            //            s.sendMsgToServer(message);

            handleMessage(userID);
            System.out.println(" [x] Done");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }

    private static void handleMessage(String userID) throws Exception {
        //Get all real-time server nodes the user is connecting
        Set<String> addresses = getNodeAddressByUserID(userID);
        for(String address:addresses){
            String realTimeNode =
                    "http://" + address + "/push";
            logger.info(uuid, "Real-time Node address:{}", realTimeNode);
            HttpRequest.sendPost(realTimeNode, "userID=" + userID);
        }
    }

    private static Set<String> getNodeAddressByUserID(String userID)
            throws Exception {
        Set<String> result;
        if(memClient.get(userID)==null){
            result = null;
        }
        else{
            String jsonObj = GsonUtils.getGson().toJson(memClient.get(userID));
            result = GsonUtils.getGson().fromJson(
                    jsonObj.substring(1, jsonObj.length() - 1)
                            .replace("\\", ""), new TypeToken<Set<String>>() {
                    }.getType());
        }
        return result;
    }

}
