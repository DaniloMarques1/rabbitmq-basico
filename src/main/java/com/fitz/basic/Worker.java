package com.fitz.basic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class Worker {
    private final static String QUEUE_NAME = "pdist_example";

    public static void main(String[] args) throws Exception {
        System.out.println("Worker DANILO MARQUEST DE OLIVEIRA");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        channel.basicQos(1);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.printf(" [x] Received '%s'\n", message);

            try {
                System.out.println("Starting worker");
                doWork(message);
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            } finally {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println(" [X] DONE");
            }
        };

        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }

    private static void doWork(String task) throws InterruptedException {
        long count = task.chars().filter(c -> c == '.').count();
        System.out.printf("Count %d\n", count);
        Thread.sleep(count * 100);
    }

}
