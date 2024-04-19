package ahmed.test.rabbitmqtest.receiver;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class RabbitMQConsumer {

    @RabbitListener(queues = "notifiy-by-email")
    public void sendEmail(String message) {
        System.out.println("Send email Recseeived message: " + message);
    }
    
    @RabbitListener(queues = "notifiy-by-sms")
    public void sendSms(String message) {
        System.out.println("Send Sms Recseeived message: " + message);
    }
}