package com.bizzan.bitrade.config;

import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.Trader.CoinTrader;
import com.bizzan.bitrade.Trader.CoinTraderFactory;
import com.bizzan.bitrade.entity.ExchangeOrder;
import com.bizzan.bitrade.entity.ExchangeOrderDetail;
import com.bizzan.bitrade.service.ExchangeOrderDetailService;
import com.bizzan.bitrade.service.ExchangeOrderService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 【故障恢复·面试重点】Spring 容器启动完成后，重建内存订单簿
 *
 * 【为什么需要它】撮合引擎是纯内存的，进程重启后订单簿全丢。
 *   但所有挂单在下单时已落库(exchange_order 表, status=TRADING)，
 *   所以重启时从 DB 捞出所有"交易中"的订单，重新喂给撮合器重建订单簿。
 *
 * 【恢复流程】
 *   1. 查该交易对所有 TRADING 状态订单
 *   2. 用成交明细表(exchange_order_detail)重算每个订单的"已成交量/已成交额"
 *      （因为订单表里的 tradedAmount 只在订单完成时才更新，进行中的部分成交没回写，
 *        所以要从明细表聚合还原 —— 这是"事件溯源"的简化版：内存状态可从日志重建）
 *   3. 未完成的订单重新 trade() 一遍挂回订单簿（注意：此时 ready=false，不发盘口消息）
 *   4. 恢复期间发现"实际已完成但状态没更新"的订单 → 补发完成消息
 *   5. setReady(true) → 开始接收新订单
 *
 * 【恢复期间新订单怎么办】ExchangeOrderConsumer 检查 trader.getReady()==false 时
 *   直接撤单退款 —— 简单粗暴，恢复期间用户下单会被撤销（可用性妥协）
 *
 * 【缺点】恢复时间随挂单量线性增长（每单还要查明细表，N+1 查询），
 *   大交易对几万挂单时恢复要几分钟；顶级交易所用"内存快照+WAL日志"实现秒级恢复
 */
@Component
public class CoinTraderEvent implements ApplicationListener<ContextRefreshedEvent> {
    private Logger log = LoggerFactory.getLogger(CoinTraderEvent.class);
    @Autowired
    CoinTraderFactory coinTraderFactory;
    @Autowired
    private ExchangeOrderService exchangeOrderService;
    @Autowired
    private ExchangeOrderDetailService exchangeOrderDetailService;
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        log.info("======initialize coinTrader======");
        // coinTraderFactory.getTraderMap();
        Map<String,CoinTrader> traders = coinTraderFactory.getTraderMap();
        traders.forEach((symbol,trader) ->{
        	log.info("======CoinTrader Process: " + symbol + "======");
            List<ExchangeOrder> orders = exchangeOrderService.findAllTradingOrderBySymbol(symbol);
            log.info("Initialize: find all trading orders, total count( " + orders.size() + ")");
            List<ExchangeOrder> tradingOrders = new ArrayList<>();
            List<ExchangeOrder> completedOrders = new ArrayList<>();
            orders.forEach(order -> {
                BigDecimal tradedAmount = BigDecimal.ZERO;
                BigDecimal turnover = BigDecimal.ZERO;
                List<ExchangeOrderDetail> details = exchangeOrderDetailService.findAllByOrderId(order.getOrderId());

                for(ExchangeOrderDetail trade:details){
                    tradedAmount = tradedAmount.add(trade.getAmount());
                    turnover = turnover.add(trade.getAmount().multiply(trade.getPrice()));
                }
                order.setTradedAmount(tradedAmount);
                order.setTurnover(turnover);
                if(!order.isCompleted()){
                    tradingOrders.add(order);
                }
                else{
                    completedOrders.add(order);
                }
            });
            log.info("Initialize: tradingOrders total count( " + tradingOrders.size() + ")");
            try {
				trader.trade(tradingOrders);
			} catch (ParseException e) {
				e.printStackTrace();
				log.info("异常：trader.trade(tradingOrders);");
			}
            //判断已完成的订单发送消息通知
            if(completedOrders.size() > 0){
            	log.info("Initialize: completedOrders total count( " + tradingOrders.size() + ")");
                kafkaTemplate.send("exchange-order-completed", JSON.toJSONString(completedOrders));
            }
            trader.setReady(true);
        });
    }

}
