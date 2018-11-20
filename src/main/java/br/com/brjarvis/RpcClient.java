package br.com.brjarvis;

import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;

public class RpcClient {

    private static final String QUEUE_NAME = "rpc_queue";
    private static final int FIBONACCI_NUMBER = 8;

    public static void main(String[] args) throws Exception {
        /*
         * Criando conexão com o AMQ
         */
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        /*
         * Declarando QUEUE de requisição
         */
        System.out.println("Declaring Request QUEUE '" + QUEUE_NAME + "'");
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        /*
         * Declarando QUEUE de resposta
         */
        final String callbackQueueName = channel.queueDeclare().getQueue();
        System.out.println("Declared Response QUEUE '" + callbackQueueName + "'");
        final BasicProperties props = new BasicProperties
                .Builder()
                .replyTo(callbackQueueName)
                .build();

        final String numberStr = String.valueOf(FIBONACCI_NUMBER);

        /*
         * Publishing requested processment
         */
        channel.basicPublish("", QUEUE_NAME, props, numberStr.getBytes());
        System.out.println(" [x] Requested fibonacci value for '" + numberStr + "'");


        /*
         * Reading callback queue
         */
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) {
                final String message = new String(body);
                System.out.println("[x] Received fibonnaci value '" + message + "'");
                System.exit(1);
            }
        };

        /*
         * Listening answer
         */
        System.out.println(" [x] Listening answer on '" + callbackQueueName + "'");
        channel.basicConsume(callbackQueueName, true, consumer);
    }
}
