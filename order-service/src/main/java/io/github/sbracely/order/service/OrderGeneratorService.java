package io.github.sbracely.order.service;

import io.github.sbracely.base.domain.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class OrderGeneratorService {

    private static Random RAND = new Random();
    private AtomicLong id = new AtomicLong();
    private Executor executor;
    private KafkaTemplate<Long, Order> template;

    public OrderGeneratorService(Executor executor, KafkaTemplate<Long, Order> template) {
        this.executor = executor;
        this.template = template;
    }

    @Async
    public void generate() {
        // 批量构造随机订单，用于压测 Saga 主流程与失败补偿路径
        for (int i = 0; i < 10000; i++) {
            int x = RAND.nextInt(5) + 1;
            // customerId/productId 采用固定范围随机值，与初始化数据规模对齐
            Order o = new Order(id.incrementAndGet(), RAND.nextLong(100) + 1, RAND.nextLong(100) + 1, "NEW");
            // price 与 productCount 同步变化，便于观察支付与库存分支的拒绝概率
            o.setPrice(100 * x);
            o.setProductCount(x);
            template.send("orders", o.getId(), o);
        }
    }
}
