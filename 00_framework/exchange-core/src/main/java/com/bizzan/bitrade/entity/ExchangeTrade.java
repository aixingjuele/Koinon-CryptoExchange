package com.bizzan.bitrade.entity;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 撮合交易信息（成交记录）
 *
 * 在撮合流程中的角色：撮合引擎每撮合成功一笔（买单与卖单价格交叉）就产生一条 ExchangeTrade，
 * 通过 Kafka "exchange-trade" 发往 market 模块，由 ExchangeTradeConsumer 消费后做资金结算
 * （买方扣冻结、卖方加收入、记流水、返佣）和行情推送。
 * 注意：本类不是 JPA 实体（无 @Entity），是撮合引擎与结算模块之间的消息载体（DTO）。
 */
@Data
public class ExchangeTrade implements Serializable{
    private String symbol; // 交易对
    private BigDecimal price; // 成交价（按价格优先原则，取先到订单簿那一方的价格）
    private BigDecimal amount; // 成交量
    private BigDecimal buyTurnover; // 买方成交额（买方付出的计价币）
    private BigDecimal sellTurnover; // 卖方成交额（卖方收到的计价币，与 buyTurnover 可能因精度略有差异）
    private ExchangeOrderDirection direction; // 吃单方向（taker 方向），用于行情判断涨跌
    private String buyOrderId; // 买方订单ID，结算时据此找到买方钱包扣冻结
    private String sellOrderId; // 卖方订单ID，结算时据此找到卖方钱包加收入
    private Long time; // 成交时间
    @Override
    public String toString() {
        return  JSON.toJSONString(this);
    }
}
