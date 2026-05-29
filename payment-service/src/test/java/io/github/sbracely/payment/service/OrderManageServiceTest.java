package io.github.sbracely.payment.service;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.payment.domain.Customer;
import io.github.sbracely.payment.repository.CustomerRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderManageServiceTest {

    @Mock
    private CustomerRepository repository;
    @Mock
    private KafkaTemplate<Long, Order> template;
    @InjectMocks
    private OrderManageService service;

    @Test
    void reserveAcceptsExactAvailableAmount() {
        Customer customer = new Customer(5L, "Customer", 100, 0);
        when(repository.findById(customer.getId())).thenReturn(Optional.of(customer));

        Order order = new Order(4L, customer.getId(), 9L, 3, 100);

        service.reserve(order);

        assertEquals("ACCEPT", order.getStatus());
        assertEquals("PAYMENT", order.getSource());
        assertEquals(0, customer.getAmountAvailable());
        assertEquals(100, customer.getAmountReserved());
        verify(repository).save(customer);
        verify(template).send("payment-orders", order.getId(), order);
    }
}
