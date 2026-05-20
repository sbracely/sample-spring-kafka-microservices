package io.github.sbracely.payment.service;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.payment.domain.Customer;
import io.github.sbracely.payment.repository.CustomerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderManageService {

    private static final String SOURCE = "PAYMENT";
    private static final Logger LOG = LoggerFactory.getLogger(OrderManageService.class);
    private final CustomerRepository repository;
    private final KafkaTemplate<Long, Order> template;

    public OrderManageService(CustomerRepository repository, KafkaTemplate<Long, Order> template) {
        this.repository = repository;
        this.template = template;
    }

    public void reserve(Order order) {
        Customer customer = repository.findById(order.getCustomerId()).orElseThrow();
        LOG.info("Found: {}", customer);

        if (order.getPrice() < customer.getAmountAvailable()) {
            order.setStatus("ACCEPT");
            customer.setAmountReserved(customer.getAmountReserved() + order.getPrice());
            customer.setAmountAvailable(customer.getAmountAvailable() - order.getPrice());
            repository.save(customer);
        } else {
            order.setStatus("REJECT");
        }

        order.setSource(SOURCE);
        template.send("payment-orders", order.getId(), order);
        LOG.info("Sent: {}", order);
    }

    public void confirm(Order order) {
        Customer customer = repository.findById(order.getCustomerId()).orElseThrow();
        LOG.info("Found: {}", customer);

        if ("CONFIRMED".equals(order.getStatus())) {
            customer.setAmountReserved(customer.getAmountReserved() - order.getPrice());
            repository.save(customer);
        } else if ("ROLLBACK".equals(order.getStatus()) && !SOURCE.equals(order.getSource())) {
            customer.setAmountReserved(customer.getAmountReserved() - order.getPrice());
            customer.setAmountAvailable(customer.getAmountAvailable() + order.getPrice());
            repository.save(customer);
        }
    }
}

