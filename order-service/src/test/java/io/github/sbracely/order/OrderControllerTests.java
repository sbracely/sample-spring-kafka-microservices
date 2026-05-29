package io.github.sbracely.order;

import io.github.sbracely.base.domain.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.kafka.consumer.auto-offset-reset=earliest",
                "spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.LongDeserializer",
                "spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer",
                "spring.kafka.consumer.properties.spring.json.trusted.packages=*",
                "spring.kafka.consumer.properties.spring.json.value.default.type=io.github.sbracely.base.domain.Order"
        }
)
@EmbeddedKafka(
        topics = {"orders", "payment-orders", "stock-orders"},
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@AutoConfigureTestRestTemplate
public class OrderControllerTests {

    @Autowired
    TestRestTemplate restTemplate;
    @Autowired
    private KafkaTemplate<Long, Order> template;
    @Autowired
    private ConsumerFactory<Long, Order> factory;

    @Test
    void add() {
        Order order = new Order(1L, 1L, 1L, 10, 100);
        order = restTemplate.postForObject("/orders", order, Order.class);
        assertNotNull(order);
        assertEquals(1L, order.getId());

        template.setConsumerFactory(factory);
        ConsumerRecord<Long, Order> record = template.receive("orders", 0, 0, Duration.ofSeconds(5));
        assertNotNull(record);
        assertNotNull(record.value());
        assertEquals(1L, record.value().getId());
    }
}
