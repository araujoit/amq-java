package br.com.brjarvis;

import com.rabbitmq.client.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RpcServer {
    private final static String QUEUE_NAME = "rpc_queue";

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
        System.out.println("Declaring Request QUEUE '" + QUEUE_NAME + "'");
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        System.out.println(" [x] Waiting for messages. To exit press CTRL+C");

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                /*
                 * Obtaining request number
                 */
                final String nStr = new String(body, StandardCharsets.UTF_8);
                System.out.println(" [x] Calculating fibonnaci value for '" + nStr + "'");

                final int n = Integer.parseInt(nStr);

                /*
                 * Calculating fibonacci value
                 */
                final int fibonacciValue = new FibonacciRpcClient().call(n);
                System.out.println(" [x] Fibonacci value for '" + nStr + "' = '" + fibonacciValue + "'");

                /*
                 * Obtaining answer QUEUE name
                 */
                final String queueReplyTo = properties.getReplyTo();

                /*
                 * Publishing fibonacci value
                 */
                byte[] fibonacciBytes = String.valueOf(fibonacciValue).getBytes();
                System.out.println(" [x] Publishing on queue '" + queueReplyTo + "'");

                channel.basicPublish("", queueReplyTo, properties, fibonacciBytes);
                System.out.println(" [x] Published answer on '" + queueReplyTo + "'");

                /*
                 * Acknowledging message
                 */
                channel.basicAck(envelope.getDeliveryTag(), false);
                System.out.println(" [x] Acknowledged message '" + envelope.getDeliveryTag() + "'");
            }
        };

        /*
         * Consuming messages
         */
        channel.basicConsume(QUEUE_NAME, false, consumer);
    }


    public static class FibonacciRpcClient {

        int call(int n) {
            return fib(n);
        }

        private static int fib(int n) {
            if (n == 0) return 0;
            if (n == 1) return 1;
            return fib(n - 1) + fib(n - 2);
        }
    }
}
