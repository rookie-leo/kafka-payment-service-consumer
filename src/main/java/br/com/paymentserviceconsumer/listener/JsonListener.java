package br.com.paymentserviceconsumer.listener;

import br.com.paymentserviceconsumer.models.Payment;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class JsonListener {
    @SneakyThrows
    @KafkaListener(topics = "payments-topic", groupId = "create-group", containerFactory = "jsonContainerFactory")
    public void antiFraud(@Payload Payment payment) {
        log.info("Payment {} receveid", payment.toString());
        Thread.sleep(2000);
        log.info("Faud validation...");
        Thread.sleep(2000);
        log.info("Purchase approved");
        Thread.sleep(2000);
    }

    @SneakyThrows
    @KafkaListener(topics = "payments-topic", groupId = "pdf-group", containerFactory = "jsonContainerFactory")
    public void pdfGenerator() {
        log.info("Generate pdf...");
        Thread.sleep(2000);
    }

    @SneakyThrows
    @KafkaListener(topics = "payments-topic", groupId = "email-group", containerFactory = "jsonContainerFactory")
    public void sendEmail() {
        log.info("Sending e-mail...");
    }
}
