package com.bizzan.bitrade.entity;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.persistence.*;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

/**
 * 【面试重点】委托订单实体 —— 撮合引擎的核心数据对象。
 *
 * 在撮合流程中的角色：用户下单后，订单通过 Kafka 进入 exchange 模块的内存撮合引擎
 * （CoinTrader，每个交易对一个，内部用 TreeMap 维护买卖订单簿），
 * 撮合引擎在内存中直接修改本对象的 tradedAmount/turnover 等字段进行撮合，
 * 订单完成或取消后再异步回写 DB —— 这是"内存撮合、异步落库"的典型设计，
 * 用牺牲少量持久化实时性换取极高的撮合吞吐。
 *
 * 缺点：引擎宕机时内存中的订单状态可能丢失，需要靠订单快照/日志重放恢复
 * （顶级交易所会用 LMAX Disruptor + 事件溯源 Event Sourcing 解决）。
 */
@Data
@Entity
@Table
public class ExchangeOrder implements Serializable {
    @Id
    private String orderId; // 订单号，全局唯一，撮合与成交回报都靠它关联
    private Long memberId; // 下单用户ID
    //挂单类型
    //LIMIT_PRICE 限价单 / MARKET_PRICE 市价单：限价单按 price 撮合，市价单按对手价直接吃单
    private ExchangeOrderType type;
    //买入或卖出量，对于市价买入单表
    //注意：amount 语义随订单类型变化——限价单表示"数量"；市价买单表示"成交额"（花多少钱买），
    //因为市价买单下单时不知道成交价，只能指定总金额。这是交易所系统的常见设计，面试易混淆点。
    @Column(columnDefinition = "decimal(18,8) DEFAULT 0 ")
    private BigDecimal amount = BigDecimal.ZERO;
    //交易对符号
    private String symbol; // 如 BTC/USDT，撮合引擎按 symbol 分片，每个交易对独立订单簿
    //成交量
    //已成交量：撮合引擎每撮合一笔就累加它，tradedAmount == amount 时订单完全成交
    @Column(columnDefinition = "decimal(26,16) DEFAULT 0 ")
    private BigDecimal tradedAmount = BigDecimal.ZERO;
    //成交额，对市价买单有用
    //已成交额：市价买单用它判断是否花完了预算（turnover >= amount 即完成）
    @Column(columnDefinition = "decimal(26,16) DEFAULT 0 ")
    private BigDecimal turnover = BigDecimal.ZERO;
    //币单位
    private String coinSymbol; // 交易币（如 BTC）
    //结算单位
    private String baseSymbol; // 计价币（如 USDT）
    //订单状态
    //状态机：TRADING(交易中) -> COMPLETED(完全成交) / CANCELED(已撤销)，撮合引擎驱动状态流转
    private ExchangeOrderStatus status;
    //订单方向
    //BUY 买 / SELL 卖：决定进买盘订单簿还是卖盘订单簿
    private ExchangeOrderDirection direction;
    //挂单价格
    //限价单的委托价；市价单此字段无意义（为 0）
    @Column(columnDefinition = "decimal(18,8) DEFAULT 0 ")
    private BigDecimal price = BigDecimal.ZERO;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    //挂单时间
    private Long time;
    //交易完成时间
    private Long completedTime;
    //取消时间
    private Long canceledTime;
    //是否使用折扣 0 不使用 1使用
    private  String useDiscount ;

    private ExchangeOrderResource orderResource = ExchangeOrderResource.CUSTOMER;

    @Transient
    private List<ExchangeOrderDetail> detail;
    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    /**
     * 判断订单是否完成。
     * 巧妙点：区分了市价买单的特殊语义——市价买单的 amount 是"成交额"，
     * 所以用 turnover(已成交额) >= amount 判断花完了没有；
     * 其他订单（限价单、市价卖单）amount 是"数量"，用 tradedAmount >= amount 判断。
     */
    public boolean isCompleted(){
        if(status != ExchangeOrderStatus.TRADING) {
            return true;
        } else{
            if(type == ExchangeOrderType.MARKET_PRICE && direction == ExchangeOrderDirection.BUY){
                return amount.compareTo(turnover) <= 0;
            }
            else{
                return amount.compareTo(tradedAmount) <= 0;
            }
        }
    }
}
