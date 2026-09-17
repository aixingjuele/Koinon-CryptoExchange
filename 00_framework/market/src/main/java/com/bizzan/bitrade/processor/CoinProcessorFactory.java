package com.bizzan.bitrade.processor;


import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/*
 * 【面试要点】工厂模式 + 注册表：每个交易对(symbol)对应一个 CoinProcessor 实例，
 * 集中存放在 ConcurrentHashMap 中（Kafka 消费线程与定时线程都会并发读取，故用并发容器）。
 * 消费撮合结果时按 symbol 找到对应处理器分发，实现"交易对级别"的行情隔离。
 */
@Slf4j
public class CoinProcessorFactory {
    private ConcurrentHashMap<String, CoinProcessor> processorMap;

    public CoinProcessorFactory() {
        processorMap = new ConcurrentHashMap<>();
    }

    public void addProcessor(String symbol, CoinProcessor processor) {
        log.info("CoinProcessorFactory addProcessor = {}" + symbol);
        processorMap.put(symbol, processor);
    }

    public boolean containsProcessor(String symbol) {
    	return processorMap != null && processorMap.containsKey(symbol);
    }
    
    public CoinProcessor getProcessor(String symbol) {
        return processorMap.get(symbol);
    }

    public ConcurrentHashMap<String, CoinProcessor> getProcessorMap() {
        return processorMap;
    }
}
