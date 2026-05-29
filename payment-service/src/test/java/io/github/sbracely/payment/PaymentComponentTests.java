package io.github.sbracely.payment;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.payment.domain.Customer;
import io.github.sbracely.payment.repository.CustomerRepository;
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
        topics = "payment-orders",
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PaymentComponentTests {

    private static final Logger LOG = LoggerFactory.getLogger(PaymentComponentTests.class);

    private Customer customer1;
    private Customer customer2;

    @Autowired
    private KafkaTemplate<Long, Order> template;
    @Autowired
    private ConsumerFactory<Long, Order> factory;
    @Autowired
    private CustomerRepository repository;

    private void resetCustomers() {
        repository.deleteAll();
        customer1 = repository.save(new Customer(null, "Customer1", 200, 0));
        customer2 = repository.save(new Customer(null, "Customer2", 100, 0));
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    void eventAccept() throws ExecutionException, InterruptedException, TimeoutException {
        resetCustomers();

        Order order = new Order(1L, customer1.getId(), 1L, 10, 100);
        SendResult<Long, Order> result = template.send("orders", order.getId(), order)
                .get(1000, TimeUnit.MILLISECONDS);
        LOG.info("Sent: {}", result.getProducerRecord().value());

        template.setConsumerFactory(factory);
        ConsumerRecord<Long, Order> record = template.receive("payment-orders", 0, 0, Duration.ofSeconds(5));
        assertNotNull(record);
        assertNotNull(record.value());
        assertEquals("ACCEPT", record.value().getStatus());

        Customer customer = repository.findById(customer1.getId()).orElseThrow();
        assertEquals(100, customer.getAmountAvailable());
        assertEquals(100, customer.getAmountReserved());
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    void eventReject() throws ExecutionException, InterruptedException, TimeoutException {
        resetCustomers();

        Order order = new Order(2L, customer2.getId(), 2L, 10, 1000);
        SendResult<Long, Order> result = template.send("orders", order.getId(), order)
                .get(1000, TimeUnit.MILLISECONDS);
        LOG.info("Sent: {}", result.getProducerRecord().value());

        template.setConsumerFactory(factory);
        ConsumerRecord<Long, Order> record = template.receive("payment-orders", 0, 1, Duration.ofSeconds(5));
        assertNotNull(record);
        assertNotNull(record.value());
        assertEquals("REJECT", record.value().getStatus());
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    void eventConfirm() throws ExecutionException, InterruptedException, TimeoutException {
        resetCustomers();

        Order reserveOrder = new Order(3L, customer1.getId(), 1L, 10, 100);
        SendResult<Long, Order> reserveResult = template.send("orders", reserveOrder.getId(), reserveOrder)
                .get(1000, TimeUnit.MILLISECONDS);
        LOG.info("Sent: {}", reserveResult.getProducerRecord().value());

        template.setConsumerFactory(factory);
        ConsumerRecord<Long, Order> reserveRecord = template.receive("payment-orders", 0, 2, Duration.ofSeconds(5));
        assertNotNull(reserveRecord);
        assertEquals("ACCEPT", reserveRecord.value().getStatus());

        Order confirmOrder = new Order(3L, customer1.getId(), 1L, 10, 100);
        confirmOrder.setStatus("CONFIRMED");
        SendResult<Long, Order> confirmResult = template.send("orders", confirmOrder.getId(), confirmOrder)
                .get(1000, TimeUnit.MILLISECONDS);
        LOG.info("Sent: {}", confirmResult.getProducerRecord().value());

        Thread.sleep(3000);
        Customer customer = repository.findById(customer1.getId()).orElseThrow();
        assertEquals(100, customer.getAmountAvailable());
        assertEquals(0, customer.getAmountReserved());
    }
}
