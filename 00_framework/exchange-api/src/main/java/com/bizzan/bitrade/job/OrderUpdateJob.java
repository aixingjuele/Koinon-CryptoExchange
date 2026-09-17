package com.bizzan.bitrade.job;


import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.entity.ExchangeCoin;
import com.bizzan.bitrade.entity.ExchangeOrder;
import com.bizzan.bitrade.service.ExchangeCoinService;
import com.bizzan.bitrade.service.ExchangeOrderService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/*
 * 【面试要点】超时订单自动撤单的兜底定时任务（交易所风控/体验功能）：
 * 每 5 分钟扫描所有启用交易对中"挂单超过 maxTradingTime 仍未完全成交"的订单，
 * 发 Kafka "exchange-order-cancel" 由撮合引擎执行撤单并解冻资金。
 * 注意：扫描走 DB（exchange-api 侧），撮合引擎状态在内存（exchange 侧），
 * 两边通过 Kafka 消息驱动达到最终一致 —— 这是典型的"DB + 内存引擎"双写一致性方案；
 * 撤单请求也可能与正在发生的撮合并发，引擎侧需做幂等/状态校验。
 * 缺点：fixedRate 轮询扫描 DB，订单量大时有 DB 压力；且撤单延迟最长 5 分钟。
 */
@Component
public class OrderUpdateJob {
    @Autowired
    private ExchangeOrderService orderService;
    @Autowired
    private ExchangeCoinService coinService;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private Logger logger = LoggerFactory.getLogger(OrderUpdateJob.class);

    // 5分钟检查一次超时订单
    // 按交易对配置 maxTradingTime（<=0 表示不限制）过滤出超时挂单；
    // 不直接操作 DB 撤单，而是发消息给撮合引擎，保证"内存订单簿"是唯一权威数据源
    @Scheduled(fixedRate = 300*1000)
    public void autoCancelOrder(){
        logger.info("start autoCancelOrder...");
        List<ExchangeCoin> coinList = coinService.findAllEnabled();
        coinList.forEach(coin->{
            if(coin.getMaxTradingTime() > 0){
                List<ExchangeOrder> orders =  orderService.findOvertimeOrder(coin.getSymbol(), coin.getMaxTradingTime());
                orders.forEach(order -> {
                    // 发送消息至Exchange系统
                    kafkaTemplate.send("exchange-order-cancel", JSON.toJSONString(order));
                    logger.info("orderId:"+order.getOrderId()+",time:"+order.getTime());
                });
            }
        });
        logger.info("end autoCancelOrder...");
    }


}
