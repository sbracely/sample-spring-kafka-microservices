package io.github.sbracely.stock;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.stock.domain.Product;
import io.github.sbracely.stock.repository.ProductRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(properties = {"spring.kafka.consumer.auto-offset-reset=earliest"})
@EmbeddedKafka(
        topics = "stock-orders",
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StockComponentTests {

    private static final Logger LOG = LoggerFactory.getLogger(StockComponentTests.class);

    private Product product1;
    private Product product2;

    @Autowired
    private KafkaTemplate<Long, Order> template;
    @Autowired
    private ConsumerFactory<Long, Order> factory;
    @Autowired
    private ProductRepository repository;

    private void resetProducts() {
        repository.deleteAll();
        product1 = repository.save(new Product(null, "Product1", 100, 0));
        product2 = repository.save(new Product(null, "Product2", 50, 0));
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    void eventAccept() throws ExecutionException, InterruptedException, TimeoutException {
        resetProducts();

        Order order = new Order(1L, 1L, product1.getId(), 10, 100);
        SendResult<Long, Order> result = template.send("orders", order.getId(), order)
                .get(1000, TimeUnit.MILLISECONDS);
        LOG.info("Sent: {}", result.getProducerRecord().value());

        template.setConsumerFactory(factory);
        ConsumerRecord<Long, Order> record = template.receive("stock-orders", 0, 0, Duration.ofSeconds(5));
        assertNotNull(record);
        assertNotNull(record.value());
        assertEquals("ACCEPT", record.value().getStatus());

        Product product = repository.findById(product1.getId()).orElseThrow();
        assertEquals(90, product.getAvailableItems());
        assertEquals(10, product.getReservedItems());
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    void eventReject() throws ExecutionException, InterruptedException, TimeoutException {
        resetProducts();

        Order order = new Order(2L, 2L, product2.getId(), 1000, 1000);
        SendResult<Long, Order> result = template.send("orders", order.getId(), order)
                .get(1000, TimeUnit.MILLISECONDS);
        LOG.info("Sent: {}", result.getProducerRecord().value());

        template.setConsumerFactory(factory);
        ConsumerRecord<Long, Order> record = template.receive("stock-orders", 0, 1, Duration.ofSeconds(5));
        assertNotNull(record);
        assertNotNull(record.value());
        assertEquals("REJECT", record.value().getStatus());
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    void eventConfirm() throws ExecutionException, InterruptedException, TimeoutException {
        resetProducts();

        Order reserveOrder = new Order(3L, 1L, product1.getId(), 10, 100);
        SendResult<Long, Order> reserveResult = template.send("orders", reserveOrder.getId(), reserveOrder)
                .get(1000, TimeUnit.MILLISECONDS);
        LOG.info("Sent: {}", reserveResult.getProducerRecord().value());

        template.setConsumerFactory(factory);
        ConsumerRecord<Long, Order> reserveRecord = template.receive("stock-orders", 0, 2, Duration.ofSeconds(5));
        assertNotNull(reserveRecord);
        assertEquals("ACCEPT", reserveRecord.value().getStatus());

        Order confirmOrder = new Order(3L, 1L, product1.getId(), 10, 100);
        confirmOrder.setStatus("CONFIRMED");
        SendResult<Long, Order> confirmResult = template.send("orders", confirmOrder.getId(), confirmOrder)
                .get(1000, TimeUnit.MILLISECONDS);
        LOG.info("Sent: {}", confirmResult.getProducerRecord().value());

        Thread.sleep(3000);
        Product product = repository.findById(product1.getId()).orElseThrow();
        assertEquals(90, product.getAvailableItems());
        assertEquals(0, product.getReservedItems());
    }
}
