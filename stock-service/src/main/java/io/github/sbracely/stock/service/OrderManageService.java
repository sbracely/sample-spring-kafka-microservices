package io.github.sbracely.stock.service;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.stock.domain.Product;
import io.github.sbracely.stock.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderManageService {

    private static final String SOURCE = "STOCK";
    private static final Logger LOG = LoggerFactory.getLogger(OrderManageService.class);
    private final ProductRepository repository;
    private final KafkaTemplate<Long, Order> template;

    public OrderManageService(ProductRepository repository, KafkaTemplate<Long, Order> template) {
        this.repository = repository;
        this.template = template;
    }

    public void reserve(Order order) {
        Product product = repository.findById(order.getProductId()).orElseThrow();
        LOG.info("Found: {}", product);

        if (order.getProductCount() < product.getAvailableItems()) {
            product.setReservedItems(product.getReservedItems() + order.getProductCount());
            product.setAvailableItems(product.getAvailableItems() - order.getProductCount());
            order.setStatus("ACCEPT");
            repository.save(product);
        } else {
            order.setStatus("REJECT");
        }

        order.setSource(SOURCE);
        template.send("stock-orders", order.getId(), order);
        LOG.info("Sent: {}", order);
    }

    public void confirm(Order order) {
        Product product = repository.findById(order.getProductId()).orElseThrow();
        LOG.info("Found: {}", product);

        if ("CONFIRMED".equals(order.getStatus())) {
            product.setReservedItems(product.getReservedItems() - order.getProductCount());
            repository.save(product);
        } else if ("ROLLBACK".equals(order.getStatus()) && !SOURCE.equals(order.getSource())) {
            product.setReservedItems(product.getReservedItems() - order.getProductCount());
            product.setAvailableItems(product.getAvailableItems() + order.getProductCount());
            repository.save(product);
        }
    }
}

