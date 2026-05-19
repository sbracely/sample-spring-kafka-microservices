package io.github.sbracely.order;

import io.github.sbracely.base.domain.Order;
import io.github.sbracely.order.service.OrderManageService;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.concurrent.Executor;

@SpringBootApplication
@EnableKafkaStreams
@EnableAsync
public class OrderApp {

    private static final Logger LOG = LoggerFactory.getLogger(OrderApp.class);

    static void main(String[] args) {
        SpringApplication.run(OrderApp.class, args);
    }

    @Bean
    public NewTopic orders() {
        // 主事件流：既承载新订单（NEW），也承载最终决策结果（CONFIRMED/ROLLBACK/REJECTED）
        return TopicBuilder.name("orders")
                .partitions(3)
                .compact()
                .build();
    }

    @Bean
    public NewTopic paymentTopic() {
        // 支付服务回传分支处理结果（ACCEPT/REJECT）
        return TopicBuilder.name("payment-orders")
                .partitions(3)
                .compact()
                .build();
    }

    @Bean
    public NewTopic stockTopic() {
        // 库存服务回传分支处理结果（ACCEPT/REJECT）
        return TopicBuilder.name("stock-orders")
                .partitions(3)
                .compact()
                .build();
    }

    @Autowired
    OrderManageService orderManageService;

    @Bean
    public KStream<Long, Order> stream(StreamsBuilder builder) {
        JacksonJsonSerde<Order> orderSerde = new JacksonJsonSerde<>(Order.class);
        Duration joinWindow = Duration.ofSeconds(10);
        Duration legacyGrace = Duration.ofMillis(Math.max(Duration.ofDays(1).toMillis() - 2 * joinWindow.toMillis(), 0L));
        // payment-orders 作为 join 的左流，key=订单ID，value=Order(JSON)
        KStream<Long, Order> stream = builder
                .stream("payment-orders", Consumed.with(Serdes.Long(), orderSerde));

        // 在时间窗口内按订单ID join 支付与库存两个分支结果，交给编排逻辑计算最终状态
        stream.join(
                        builder.stream("stock-orders"),
                        orderManageService::confirm,
                        JoinWindows.ofTimeDifferenceAndGrace(joinWindow, legacyGrace),
                        StreamJoined.with(Serdes.Long(), orderSerde, orderSerde))
                .peek((k, o) -> LOG.info("Output[{}]: {}", k, o))
                // 把最终状态重新写回 orders，供下游服务执行确认或补偿
                .to("orders");

        return stream;
    }

    @Bean
    public KTable<Long, Order> table(StreamsBuilder builder) {
        KeyValueBytesStoreSupplier store =
                Stores.persistentKeyValueStore("orders");
        JacksonJsonSerde<Order> orderSerde = new JacksonJsonSerde<>(Order.class);
        // 读取 orders 全量事件流并物化成本地可查询 KTable
        KStream<Long, Order> stream = builder
                .stream("orders", Consumed.with(Serdes.Long(), orderSerde));
        return stream.toTable(Materialized.<Long, Order>as(store)
                .withKeySerde(Serdes.Long())
                .withValueSerde(orderSerde));
    }

    @Bean
    public Executor taskExecutor() {
        // @Async 发送消息时使用独立线程池，避免阻塞 Web/Kafka 主线程
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(5);
        executor.setThreadNamePrefix("kafkaSender-");
        executor.initialize();
        return executor;
    }
}
