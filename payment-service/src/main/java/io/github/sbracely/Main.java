package io.github.sbracely;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.payment.domain.Customer;
import io.github.sbracely.payment.repository.CustomerRepository;
import io.github.sbracely.payment.service.OrderManageService;
import jakarta.annotation.PostConstruct;
import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.Random;

@SpringBootApplication
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    private final OrderManageService orderManageService;
    private final CustomerRepository repository;

    public Main(OrderManageService orderManageService, CustomerRepository repository) {
        this.orderManageService = orderManageService;
        this.repository = repository;
    }

    @KafkaListener(id = "orders", topics = "orders", groupId = "payment")
    public void onEvent(Order order) {
        LOG.info("Received: {}", order);
        if ("NEW".equals(order.getStatus())) {
            orderManageService.reserve(order);
        } else {
            orderManageService.confirm(order);
        }
    }

    @PostConstruct
    public void generateData() {
        Random random = new Random();
        Faker faker = new Faker();
        for (int i = 0; i < 100; i++) {
            int amount = random.nextInt(200, 10000);
            Customer customer = new Customer(null, faker.name().fullName(), amount, 0);
            repository.save(customer);
        }
    }
}
