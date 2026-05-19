package io.github.sbracely.order.controller;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.order.service.OrderGeneratorService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger LOG = LoggerFactory.getLogger(OrderController.class);
    private final AtomicLong id = new AtomicLong();
    private final KafkaTemplate<Long, Order> template;
    private final StreamsBuilderFactoryBean kafkaStreamsFactory;
    private final OrderGeneratorService orderGeneratorService;

    public OrderController(KafkaTemplate<Long, Order> template,
                           StreamsBuilderFactoryBean kafkaStreamsFactory,
                           OrderGeneratorService orderGeneratorService) {
        this.template = template;
        this.kafkaStreamsFactory = kafkaStreamsFactory;
        this.orderGeneratorService = orderGeneratorService;
    }

    @PostMapping
    public Order create(@RequestBody Order order) {
        // 在编排入口统一分配订单ID，作为 Kafka key 用于后续跨服务关联
        order.setId(id.incrementAndGet());
        // 发到 orders 主题后，由 payment/stock 两个服务并行消费
        template.send("orders", order.getId(), order);
        LOG.info("Sent: {}", order);
        return order;
    }

    @PostMapping("/generate")
    public boolean generate() {
        // 异步批量生成测试订单，用于观察 Saga 流程与拒绝/回滚路径
        orderGeneratorService.generate();
        return true;
    }

    @GetMapping
    public List<Order> all() {
        List<Order> orders = new ArrayList<>();
        KafkaStreams kafkaStreams = kafkaStreamsFactory.getKafkaStreams();
        if (kafkaStreams == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka Streams is not ready yet");
        }

        // 从 Kafka Streams 的本地 state store 查询聚合后的订单视图
        ReadOnlyKeyValueStore<Long, Order> store = kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                "orders",
                QueryableStoreTypes.keyValueStore()));
        // 迭代 state store 全量输出给 API 调用方，并在结束后及时释放迭代器资源
        try (KeyValueIterator<Long, Order> it = store.all()) {
            it.forEachRemaining(kv -> orders.add(kv.value));
        }
        return orders;
    }
}
