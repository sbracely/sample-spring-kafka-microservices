package io.github.sbracely.stock.service;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.stock.domain.Product;
import io.github.sbracely.stock.repository.ProductRepository;
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
    private ProductRepository repository;
    @Mock
    private KafkaTemplate<Long, Order> template;
    @InjectMocks
    private OrderManageService service;

    @Test
    void reserveAcceptsExactAvailableItems() {
        Product product = new Product(7L, "Product", 10, 0);
        when(repository.findById(product.getId())).thenReturn(Optional.of(product));

        Order order = new Order(3L, 2L, product.getId(), 10, 100);

        service.reserve(order);

        assertEquals("ACCEPT", order.getStatus());
        assertEquals("STOCK", order.getSource());
        assertEquals(0, product.getAvailableItems());
        assertEquals(10, product.getReservedItems());
        verify(repository).save(product);
        verify(template).send("stock-orders", order.getId(), order);
    }
}
