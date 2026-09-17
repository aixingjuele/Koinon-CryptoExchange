package com.bizzan.bitrade.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import com.bizzan.bitrade.entity.CoinThumb;
import com.bizzan.bitrade.entity.ExchangeTrade;
import com.bizzan.bitrade.entity.KLine;
import com.bizzan.bitrade.job.ExchangePushJob;

/*
 * 【面试要点】MarketHandler 实现之二（观察者模式）：通过 Spring WebSocket
 * （SimpMessagingTemplate，STOMP 协议）向 H5/PC 端推送行情。
 * 成交摘要不直接推，而是丢给 ExchangePushJob 缓冲合并后批量推（防刷爆客户端）；
 * K线每分钟才一根，频率低，故直接 convertAndSend 实时推送。
 */
@Slf4j
@Component
public class WebsocketMarketHandler implements MarketHandler{
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    @Autowired
    private ExchangePushJob pushJob;

    /**
     * 推送币种简化信息
     * @param symbol
     * @param exchangeTrade
     * @param thumb
     */
    @Override
    public void handleTrade(String symbol, ExchangeTrade exchangeTrade, CoinThumb thumb) {
        //推送缩略行情
        pushJob.addThumb(symbol,thumb);
    }

    @Override
    public void handleKLine(String symbol, KLine kLine) {
        //推送K线数据
        messagingTemplate.convertAndSend("/topic/market/kline/"+symbol,kLine);
    }
}
