package io.github.sbracely.order.service;

import io.github.sbracely.base.domain.Order;
import org.springframework.stereotype.Service;

@Service
public class OrderManageService {

    public Order confirm(Order orderPayment, Order orderStock) {
        // 以分支结果重建一个输出订单，避免直接复用输入对象造成状态污染
        Order o = new Order(orderPayment.getId(),
                orderPayment.getCustomerId(),
                orderPayment.getProductId(),
                orderPayment.getProductCount(),
                orderPayment.getPrice());
        // 两个分支都成功 -> 全局确认
        if (orderPayment.getStatus().equals("ACCEPT") &&
                orderStock.getStatus().equals("ACCEPT")) {
            o.setStatus("CONFIRMED");
        // 两个分支都失败 -> 全局拒绝（无需补偿）
        } else if (orderPayment.getStatus().equals("REJECT") &&
                orderStock.getStatus().equals("REJECT")) {
            o.setStatus("REJECTED");
        // 仅一方失败 -> 触发补偿；source 标记失败来源，供对端判断是否应回滚
        } else if (orderPayment.getStatus().equals("REJECT") ||
                orderStock.getStatus().equals("REJECT")) {
            String source = orderPayment.getStatus().equals("REJECT")
                    ? "PAYMENT" : "STOCK";
            o.setStatus("ROLLBACK");
            o.setSource(source);
        }
        return o;
    }

}
