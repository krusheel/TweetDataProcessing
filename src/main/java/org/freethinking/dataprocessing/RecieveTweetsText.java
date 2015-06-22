package org.freethinking.dataprocessing;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class RecieveTweetsText {

    private final static String QUEUE_NAME = "test1";
    private final static String hostName = "localhost";

    public static void main(String[] argv)
            throws java.io.IOException,
            java.lang.InterruptedException {

        Channel channel = getChannel(hostName);
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, consumer);

        while (true) {
          QueueingConsumer.Delivery delivery = consumer.nextDelivery();
          String message = new String(delivery.getBody());
          System.out.println(" [x] Received '" + message + "'");
        }      

    }

    private static Channel getChannel(String hostName) throws IOException {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostName);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        return channel;
    }
}
