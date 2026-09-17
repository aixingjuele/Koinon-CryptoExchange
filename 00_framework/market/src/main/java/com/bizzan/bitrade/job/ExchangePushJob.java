package com.bizzan.bitrade.job;

import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.entity.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.bizzan.bitrade.handler.NettyHandler;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/*
 * 【面试要点】行情推送的"缓冲合并层"（事件合并/批量刷新模式，类似前端防抖 throttle）。
 * 背景：撮合引擎每笔成交、每次盘口变化都会发 Kafka，如果每条消息都直接推给前端，
 * 高频交易对会把客户端刷爆（网络带宽与渲染压力都扛不住）。
 * 方案：Kafka 消费线程只把数据塞进 Map<String,List> 缓冲（addTrades/addPlates/addThumb），
 * @Scheduled 定时线程每 300ms/500ms 把缓冲数据批量推送一次并清空 ——
 * 用"牺牲几百毫秒实时性"换"推送频率上限可控"，面试常考"如何降低推送频率/削峰"。
 * 缺点：tradesQueue/plateQueue/thumbQueue 用的是非线程安全的 HashMap，
 * 定时线程遍历 entrySet 的同时消费线程 put 新 symbol，理论上可能抛
 * ConcurrentModificationException（概率低但存在），更严谨应使用 ConcurrentHashMap。
 */
@Slf4j
@Component
public class ExchangePushJob {
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    @Autowired
    private NettyHandler nettyHandler;
    private Map<String,List<ExchangeTrade>> tradesQueue = new HashMap<>();
    private Map<String,List<TradePlate>> plateQueue = new HashMap<>();
    private Map<String,List<CoinThumb>> thumbQueue = new HashMap<>();

    private Random rand = new Random();
    private Map<String, TradePlate> plateLastBuy = new HashMap<>(); // 最后一次推送的盘口，仅仅是为了虚拟推送二设立的
    private Map<String, TradePlate> plateLastSell = new HashMap<>(); // 最后一次推送的盘口，仅仅是为了虚拟推送二设立的

    public void addTrades(String symbol, List<ExchangeTrade> trades){
        List<ExchangeTrade> list = tradesQueue.get(symbol);
        if(list == null){
            list = new ArrayList<>();
            tradesQueue.put(symbol,list);
        }
        // synchronized(list)：Kafka 消费线程（生产者，此处写入）与 @Scheduled 定时线程
        // （消费者，pushTrade 中遍历并 clear）并发访问同一个 list，需互斥；
        // 锁粒度是"每个 symbol 的 list"，不同交易对之间互不阻塞，属于细粒度锁
        synchronized (list) {
            list.addAll(trades);
        }
    }

    public void addPlates(String symbol, TradePlate plate){
        List<TradePlate> list = plateQueue.get(symbol);
        if(list == null){
            list = new ArrayList<>();
            plateQueue.put(symbol,list);
        }
        // 同 addTrades：消费线程写入、定时线程读取清空，按 list 加锁
        synchronized (list) {
            list.add(plate);
        }

        if(plate.getDirection() == ExchangeOrderDirection.BUY) {
            // 更新最新盘口
            synchronized (plateLastBuy) {
                plateLastBuy.put(symbol, plate);
            }
        }
        if(plate.getDirection() == ExchangeOrderDirection.SELL) {
            // 更新最新盘口
            synchronized (plateLastSell) {
                plateLastSell.put(symbol, plate);
            }
        }
    }

    public void addThumb(String symbol, CoinThumb thumb){
        List<CoinThumb> list = thumbQueue.get(symbol);
        if(list == null){
            list = new ArrayList<>();
            thumbQueue.put(symbol,list);
        }
        // 同上：生产者(消费线程)与消费者(定时线程)对共享 list 的互斥
        synchronized (list) {
            list.add(thumb);
        }
    }


    // 每 300ms 批量推送一次成交明细：一个周期内同一 symbol 的多笔成交合并成一次推送
    @Scheduled(fixedRate = 300)
    public void pushTrade(){
        Iterator<Map.Entry<String,List<ExchangeTrade>>> entryIterator = tradesQueue.entrySet().iterator();
        while (entryIterator.hasNext()){
            Map.Entry<String,List<ExchangeTrade>> entry =  entryIterator.next();
            String symbol = entry.getKey();
            List<ExchangeTrade> trades = entry.getValue();
            if(trades.size() > 0){
                synchronized (trades) {
                    messagingTemplate.convertAndSend("/topic/market/trade/" + symbol, trades);
                    trades.clear();
                }
            }
        }
    }

    /*
     * 每 500ms 批量推送一次盘口。
     * 【面试要点】hasPushAskPlate/hasPushBidPlate 标志：一个周期内买盘只推一版、
     * 卖盘只推一版（盘口是"全量快照"语义，同方向旧快照没有推送价值），进一步合并推送量。
     */
    @Scheduled(fixedDelay = 500)
    public void pushPlate(){
        Iterator<Map.Entry<String,List<TradePlate>>> entryIterator = plateQueue.entrySet().iterator();
        while (entryIterator.hasNext()){
            Map.Entry<String,List<TradePlate>> entry =  entryIterator.next();
            String symbol = entry.getKey();
            List<TradePlate> plates = entry.getValue();
            if(plates.size() > 0){
                boolean hasPushAskPlate = false;
                boolean hasPushBidPlate = false;
                synchronized (plates) {
                    for(TradePlate plate:plates) {
                        if(plate.getDirection() == ExchangeOrderDirection.BUY && !hasPushBidPlate) {
                            hasPushBidPlate = true;
                        }
                        else if(plate.getDirection() == ExchangeOrderDirection.SELL && !hasPushAskPlate){
                            hasPushAskPlate = true;
                        }
                        else {
                            continue;
                        }
                        //websocket推送盘口信息
                        messagingTemplate.convertAndSend("/topic/market/trade-plate/" + symbol, plate.toJSON(24));
                        //websocket推送深度信息
                        messagingTemplate.convertAndSend("/topic/market/trade-depth/" + symbol, plate.toJSON(50));
                        //netty推送
                        nettyHandler.handlePlate(symbol, plate);
                    }

                    plates.clear();
                }
            }else{
                // 【重要】虚假盘口（造市/刷量行为）：盘口本周期没有任何变化时，随机篡改
                // 上一次推送的盘口数据（数量×0.5、价格取相邻档中间值）再推给前端，
                // 人为制造"交易活跃"的假象。这属于交易所流动性造假手段，面试时可作谈资，
                // 但生产环境应删除此分支 —— 它推送的是不存在的假挂单，有合规风险。
                // 不管盘口有没有变化，都推送一下数据，显得盘口交易很活跃的样子(这里获取到的盘口有可能是买盘，也可能是卖盘)
                TradePlate plateBuy = plateLastBuy.get(symbol);
                TradePlate plateSell = plateLastSell.get(symbol);
                if(plateBuy != null) {
                    // 随机修改盘口数据，然后推送
                    List<TradePlateItem> list = plateBuy.getItems();
                    if(list.size() > 9) { // 只要大于随机数的种子就行，这里的4是随意设置的
                        int randInt = rand.nextInt(9);
                        list.get(randInt).setAmount(list.get(randInt).getAmount().multiply(BigDecimal.valueOf(0.5))); // 随机挑选一个，让盘口订单数量+50%
                        if(randInt > 0 && randInt < list.size() - 1) { // 如果在中间，就可以变动价格
                            // 价格取前后价格居中的价格
                            list.get(randInt).setPrice(list.get(randInt - 1).getPrice().add(list.get(randInt + 1).getPrice()).divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_DOWN));
                        }
                        //websocket推送盘口信息
                        messagingTemplate.convertAndSend("/topic/market/trade-plate/" + symbol, plateBuy.toJSON(24));
                        //websocket推送深度信息
                        messagingTemplate.convertAndSend("/topic/market/trade-depth/" + symbol, plateBuy.toJSON(50));
                        //netty推送
                        nettyHandler.handlePlate(symbol, plateBuy);
                    }
                }
                if(plateSell != null) {
                    // 随机修改盘口数据，然后推送
                    List<TradePlateItem> list = plateSell.getItems();
                    if(list.size() > 9) { // 只要大于随机数的种子就行，这里的4是随意设置的
                        int randInt = rand.nextInt(9);
                        list.get(randInt).setAmount(list.get(randInt).getAmount().multiply(BigDecimal.valueOf(0.5))); // 随机挑选一个，让盘口订单数量+50%
                        if(randInt > 0 && randInt < list.size() - 1) { // 如果在中间，就可以变动价格
                            // 价格取前后价格居中的价格
                            list.get(randInt).setPrice(list.get(randInt - 1).getPrice().add(list.get(randInt + 1).getPrice()).divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_DOWN));
                        }
                        //websocket推送盘口信息
                        messagingTemplate.convertAndSend("/topic/market/trade-plate/" + symbol, plateSell.toJSON(24));
                        //websocket推送深度信息
                        messagingTemplate.convertAndSend("/topic/market/trade-depth/" + symbol, plateSell.toJSON(50));
                        //netty推送
                        nettyHandler.handlePlate(symbol, plateSell);
                    }
                }
            }
        }
    }

    // 每 300ms 推送一次行情摘要：只取缓冲里最后一个 CoinThumb（最新值语义，旧值直接丢弃）
    @Scheduled(fixedRate = 300)
    public void pushThumb(){
        Iterator<Map.Entry<String,List<CoinThumb>>> entryIterator = thumbQueue.entrySet().iterator();
        log.info("thumbQueue::::"+ JSON.toJSONString(thumbQueue));
        while (entryIterator.hasNext()){
            Map.Entry<String,List<CoinThumb>> entry =  entryIterator.next();
            String symbol = entry.getKey();
            List<CoinThumb> thumbs = entry.getValue();
            if(thumbs.size() > 0){
                synchronized (thumbs) {
                    messagingTemplate.convertAndSend("/topic/market/thumb",thumbs.get(thumbs.size() - 1));
                    thumbs.clear();
                }
            }
        }
    }
}
