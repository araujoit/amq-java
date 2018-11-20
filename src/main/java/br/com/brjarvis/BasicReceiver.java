package br.com.brjarvis;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class BasicReceiver {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {
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
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [x] Waiting for messages. To exit press CTRL+C");

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                /*
                 * Converting message bytes
                 */
                final String message = new String(body, StandardCharsets.UTF_8);
                /*
                 * Printing received message
                 */
                System.out.println(" [x] Received '" + message + "'");
            }
        };

        /*
         * Consuming messages
         */
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}
