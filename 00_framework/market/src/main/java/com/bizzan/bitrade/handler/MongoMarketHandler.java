package com.bizzan.bitrade.handler;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import com.bizzan.bitrade.entity.CoinThumb;
import com.bizzan.bitrade.entity.ExchangeTrade;
import com.bizzan.bitrade.entity.KLine;

/*
 * 【面试要点】MarketHandler 实现之一（观察者模式）：把成交明细和K线持久化到 MongoDB。
 * 按 symbol 分集合（exchange_trade_BTC_USDT）、K线再按周期分集合
 * （exchange_kline_BTC_USDT_1min）—— 按业务维度分表，避免单集合过大。
 * 选 MongoDB 的原因：行情数据写多读多、结构灵活、无强事务需求。
 */
@Component
public class MongoMarketHandler implements MarketHandler {
    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public void handleTrade(String symbol, ExchangeTrade exchangeTrade, CoinThumb thumb) {
        mongoTemplate.insert(exchangeTrade, "exchange_trade_" + symbol);
    }

    @Override
    public void handleKLine(String symbol, KLine kLine) {
        mongoTemplate.insert(kLine,"exchange_kline_"+symbol+"_"+kLine.getPeriod());
    }
}
