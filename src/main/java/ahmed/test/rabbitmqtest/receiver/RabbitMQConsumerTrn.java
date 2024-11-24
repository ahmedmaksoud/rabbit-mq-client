package ahmed.test.rabbitmqtest.receiver;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class RabbitMQConsumerTrn {


    private final ConcurrentHashMap<String, Integer> lastProcessedSequence = new ConcurrentHashMap<>();


    @RabbitListener(queues = "notifiy-trn_created_queue")
    public void sendSms(
        //    String messageTxt,
        Message message,
        @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
        @Header(name = "transactionId", required = false) String transactionId,
        @Header(name = "sequenceNumber", required = false) Integer sequenceNumber,
        com.rabbitmq.client.Channel channel) {
            try {
                if (transactionId == null || sequenceNumber == null) {
                    System.err.println("Missing required headers. Rejecting message.");
                    channel.basicNack(deliveryTag, false, false); // Reject message
                    return;
                }

                // Ensure order
//                int lastSequence = lastProcessedSequence.getOrDefault(transactionId, -1);
//                if (sequenceNumber == 1 ||  sequenceNumber == lastSequence + 1) {
//                    processMessage(message.getBody(), transactionId, sequenceNumber);
//                    lastProcessedSequence.put(transactionId, sequenceNumber);
//                    channel.basicAck(deliveryTag, false); // Acknowledge the message
//                } else {
//                    System.out.printf("Out-of-order message: transactionId=%s, sequenceNumber=%d%n",
//                            transactionId, sequenceNumber);
//                    // Optionally buffer the message for retry
//                    channel.basicNack(deliveryTag, false, true); // Requeue message
//                }
                processMessage(message.getBody(), transactionId, sequenceNumber);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error processing message: " + e.getMessage());
                try {
                    channel.basicNack(deliveryTag, false, false); // Reject message
                } catch (Exception ackEx) {
                    System.err.println("Error sending nack: " + ackEx.getMessage());
                }
            }

        System.out.println("notifiy-trn_created_queue message: " + message);
    }


    private void processMessage(byte[] body, String transactionId, int sequenceNumber) {
        String messageContent = new String(body);
        System.out.printf("Processing transactionId=%s, sequenceNumber=%d, message=%s%n",
                transactionId, sequenceNumber, messageContent);
    }
}