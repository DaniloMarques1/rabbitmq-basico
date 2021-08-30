package com.fitz.basic;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

public class Send {
    private final static String QUEUE_NAME = "pdist_example";

    public static void main(String[] args) {
        System.out.println("Sender DANILO MARQUEST DE OLIVEIRA");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
            channel.basicQos(1);
            String message = String.join(" ", args);
            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            System.out.printf(" [x] Sent '%s'\n", message);

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

}
