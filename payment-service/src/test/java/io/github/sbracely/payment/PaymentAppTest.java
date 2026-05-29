package io.github.sbracely.payment;

import org.springframework.boot.SpringApplication;

public class PaymentAppTest {

    public static void main(String[] args) {
        SpringApplication.from(PaymentApp::main)
                .with(KafkaContainerDevMode.class)
                .run(args);
    }

}
