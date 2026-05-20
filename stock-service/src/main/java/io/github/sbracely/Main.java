package io.github.sbracely;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.stock.domain.Product;
import io.github.sbracely.stock.repository.ProductRepository;
import io.github.sbracely.stock.service.OrderManageService;
import jakarta.annotation.PostConstruct;
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
    private final ProductRepository repository;

    public Main(OrderManageService orderManageService, ProductRepository repository) {
        this.orderManageService = orderManageService;
        this.repository = repository;
    }

    @KafkaListener(id = "orders", topics = "orders", groupId = "stock")
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
        for (int i = 0; i < 1000; i++) {
            int count = random.nextInt(10, 1000);
            Product product = new Product(null, "Product" + i, count, 0);
            repository.save(product);
        }
    }
}
