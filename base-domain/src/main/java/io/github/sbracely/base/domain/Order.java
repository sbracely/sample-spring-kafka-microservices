package io.github.sbracely.base.domain;

/**
 * 三个服务共享的订单事件契约。
 * status/source 字段共同承载 Saga 过程中的分支结果与补偿来源信息。
 */
public class Order {

    /**
     * 订单唯一标识（Kafka 消息 Key 也使用该值）。
     */
    private Long id;

    /**
     * 下单客户 ID（对应 payment-service 中的客户账户）。
     */
    private Long customerId;

    /**
     * 商品 ID（对应 stock-service 中的库存商品）。
     */
    private Long productId;

    /**
     * 购买数量（用于库存预留与回滚）。
     */
    private int productCount;

    /**
     * 订单金额（用于支付预留与回滚）。
     */
    private int price;

    /**
     * 订单状态：
     * NEW（新建）、CONFIRMED（确认）、ROLLBACK（回滚）等。
     */
    private String status;

    /**
     * 状态来源服务标识（如 PAYMENT、STOCK），
     * 用于标记分支处理结果和补偿来源。
     */
    private String source;

    public Order() {
    }

    public Order(Long id, Long customerId, Long productId, String status) {
        this.id = id;
        this.customerId = customerId;
        this.productId = productId;
        this.status = status;
    }

    public Order(Long id, Long customerId, Long productId, int productCount, int price) {
        this.id = id;
        this.customerId = customerId;
        this.productId = productId;
        this.productCount = productCount;
        this.price = price;
        this.status = "NEW";
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Long customerId) {
        this.customerId = customerId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getProductCount() {
        return productCount;
    }

    public void setProductCount(int productCount) {
        this.productCount = productCount;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", customerId=" + customerId +
                ", productId=" + productId +
                ", productCount=" + productCount +
                ", price=" + price +
                ", status='" + status + '\'' +
                ", source='" + source + '\'' +
                '}';
    }
}