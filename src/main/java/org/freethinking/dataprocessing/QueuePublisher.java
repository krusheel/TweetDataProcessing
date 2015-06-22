package org.freethinking.dataprocessing;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class QueuePublisher {

    private static Logger logger = LoggerFactory.getLogger(QueuePublisher.class);

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private String queueName;

    public QueuePublisher(String hostName, String queueName) throws IOException {

        factory = new ConnectionFactory();
        factory.setHost(hostName);

        try {
            connection = factory.newConnection();
            channel = connection.createChannel();            
            channel.queueDeclare(queueName, false, false, false, null);
            this.queueName = queueName;
        } catch (IOException e) {
            logger.error("Problem Encounterd while getting connection");
            throw e;
        }

    }

    public void publishToRabbitMQ(String message) throws IOException {

        try {            
            channel.basicPublish("", queueName, null, message.getBytes());
        } catch (IOException e) {
            logger.error("Problem Encounterd while publishing message {}", message);
            throw e;
        }
    }
    
    public void close() throws IOException {
        channel.close();
        connection.close();        
    }

}
