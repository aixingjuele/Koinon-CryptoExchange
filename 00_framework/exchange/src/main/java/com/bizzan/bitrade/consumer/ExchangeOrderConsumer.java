package com.bizzan.bitrade.consumer;

import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.Trader.CoinTrader;
import com.bizzan.bitrade.Trader.CoinTraderFactory;
import com.bizzan.bitrade.entity.ExchangeOrder;
import com.bizzan.bitrade.service.ExchangeCoinService;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 【订单入口·面试重点】撮合引擎的 Kafka 消费者，是"内存撮合"与"外部世界"的唯一连接点
 *
 * ==================== 一、Kafka 如何保证顺序（面试必问）====================
 * Kafka 的顺序保证是【分区内有序】：
 *   1. 同一分区内消息严格有序（offset 递增）
 *   2. 一个分区同一时间只能被消费组内【一个消费者】消费 → 天然单线程串行
 *   3. 因此：只要"同一交易对的订单进同一分区"，撮合就是严格串行的 → 这就是
 *      撮合引擎"无锁"的前提！订单的串行化是 Kafka 分区机制帮忙实现的。
 *   【本系统的隐患】OrderController 下单时 kafkaTemplate.send("exchange-order", json)
 *   没有传 key！无 key 消息默认粘性/轮询分区，同一交易对的订单可能分散到多个分区，
 *   多分区并发消费时，同一交易对的订单可能【乱序到达】（如买单B后下先被消费）。
 *   正确姿势：send("exchange-order", symbol, json)，用 symbol 做 key 哈希到固定分区。
 *   （不过乱序对本系统撮合结果正确性影响不大——每笔订单独立撮合，只影响成交先后公平性；
 *     真正危险的是"下单"与"撤单"乱序，所以撤单 topic 同理应注意）
 *
 * ==================== 二、消费端配置（见 KafkaConsumerConfiguration）====================
 *   - enable.auto.commit=false + Spring 容器批量提交(AckMode.BATCH)：处理完一批才提交 offset
 *   - 语义是 at-least-once：消费成功但提交前宕机 → 重启后重复消费 → 【下游必须幂等】
 *   - concurrency=9：9 个消费线程，要求 topic 分区数 >= 9 才能用满，否则有线程空转
 *
 * ==================== 三、幂等性如何保证（面试必问）====================
 *   1. 下单消息重复消费：同一订单会被撮合两次？→ 不会。因为重复消费时订单已在订单簿中，
 *      但注意：本代码【没有】做订单去重！严格说如果 exchange-order 消息重复投递，
 *      同一 orderId 会被重复撮合（资金在下单时已冻结，重复撮合会导致重复成交）。
 *      实际依赖：① Kafka 重复投递概率低 ② 上游下单接口的防重（DB 主键 orderId）
 *      更严谨的做法：撮合前检查订单簿中是否已存在该 orderId（findOrder），存在则跳过。
 *   2. 撮合结果(exchange-trade/exchange-order-completed)重复：下游 market 模块靠
 *      DB 状态机幂等（见 ExchangeTradeConsumer 注释）
 *
 * ==================== 四、异常兜底====================
 *   撮合抛异常 → 发 exchange-order-cancel-success 撤单消息 → 下游退冻结资金。
 *   保证"撮合失败不丢用户钱"（资金在下单时已冻结，必须退回）。
 */
@Slf4j
@Component
public class ExchangeOrderConsumer {

    @Autowired
    private CoinTraderFactory traderFactory;

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    /**
     * 消费新订单（批量模式，maxPollRecords=50 一批）
     * 【单线程串行的关键】同一分区的这一批 records 由同一个线程顺序执行，
     *   trader.trade(order) 逐笔调用 → 同一交易对撮合串行化
     */
    @KafkaListener(topics = "exchange-order",containerFactory = "kafkaListenerContainerFactory")
    public void onOrderSubmitted(List<ConsumerRecord<String,String>> records){
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String,String> record  = records.get(i);
            log.info("接收订单>>topic={},value={},size={}",record.topic(),record.value(),records.size());
            ExchangeOrder order = JSON.parseObject(record.value(), ExchangeOrder.class);
            if(order == null){
                return ;
            }
            //按交易对路由到对应的撮合器实例（一个交易对一个 CoinTrader）
            CoinTrader trader = traderFactory.getTrader(order.getSymbol());
            //如果当前币种交易暂停会自动取消订单
            if (trader.isTradingHalt() || !trader.getReady()) {
                //撮合器未准备完成（启动重建订单簿中），撤回当前等待的订单 → 下游退冻结资金
                kafkaTemplate.send("exchange-order-cancel-success", JSON.toJSONString(order));
            } else {
                try {
                    long startTick = System.currentTimeMillis();
                    //【核心】内存撮合，纯内存操作，微秒级完成
                    trader.trade(order);
                    log.info("complete trade,{}ms used!", System.currentTimeMillis() - startTick);
                } catch (Exception e) {
                    e.printStackTrace();
                    log.info("====交易出错，退回订单===",e);
                    //异常兜底：撮合失败撤单退款，保证用户资金不丢
                    kafkaTemplate.send("exchange-order-cancel-success", JSON.toJSONString(order));
                }
            }
        }
    }

    /**
     * 消费撤单请求
     * 【撤单链路】用户撤单 → exchange-api 发本 topic → 撮合器从订单簿移除
     *   → 发 exchange-order-cancel-success → market 模块消费 → DB 改状态 + 解冻资金
     * 【为什么撤单也要走 Kafka 而不是直接调撮合器？】
     *   撮合引擎(exchange)与下单服务(exchange-api)是两个独立部署的进程，
     *   跨进程只能通过消息通信；且走 Kafka 后撤单请求也进入分区串行队列，
     *   与下单请求统一排队，避免并发时序问题（统一串行化入口的设计思想）
     */
    @KafkaListener(topics = "exchange-order-cancel",containerFactory = "kafkaListenerContainerFactory")
    public void onOrderCancel(List<ConsumerRecord<String,String>> records){
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String,String> record  = records.get(i);
            log.info("取消订单topic={},key={},size={}",record.topic(),record.key(),records.size());
            ExchangeOrder order = JSON.parseObject(record.value(), ExchangeOrder.class);
            if(order == null){
                return ;
            }
            CoinTrader trader = traderFactory.getTrader(order.getSymbol());
            if(trader.getReady()) {
                try {
                    ExchangeOrder result = trader.cancelOrder(order);
                    if (result != null) {
                        kafkaTemplate.send("exchange-order-cancel-success", JSON.toJSONString(result));
                    }
                }catch (Exception e){
                    log.info("====取消订单出错===",e);
                    e.printStackTrace();
                }
            }
        }
    }
}
