package com.bizzan.bitrade.entity;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;

/**
 * 订单成交明细。
 *
 * 在撮合流程中的角色：一个订单可能分多笔成交（部分成交），每笔成交生成一条明细，
 * 落库到 MongoDB（@Document），供用户查询"我的订单分几笔、各是什么价格成交的"。
 * 用 MongoDB 而非 MySQL 存明细：明细是只增不改的流水型数据，写入量大、无事务需求，
 * 适合文档库；这也是"交易流水与核心账务分离存储"的常见做法。
 */
@Data
@Document(collection = "exchange_order_detail")
public class ExchangeOrderDetail {
    private String orderId; // 所属订单ID
    private BigDecimal price; // 该笔成交价
    private BigDecimal amount; // 该笔成交量
    private BigDecimal turnover; // 该笔成交额
    private BigDecimal fee; // 该笔手续费
    //成交时间
    private long time;
}
