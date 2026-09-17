package com.bizzan.bitrade.processor;

import java.util.List;

import com.bizzan.bitrade.component.CoinExchangeRate;
import com.bizzan.bitrade.entity.CoinThumb;
import com.bizzan.bitrade.entity.ExchangeTrade;
import com.bizzan.bitrade.entity.KLine;
import com.bizzan.bitrade.handler.MarketHandler;
import com.bizzan.bitrade.service.MarketService;

/*
 * 【面试要点】行情处理器接口：面向接口编程，便于替换实现（当前为 DefaultCoinProcessor）。
 * 核心方法分三类：
 *  1) 实时处理：process(成交明细) 聚合 1min K线与今日摘要；
 *  2) 定时调度：autoGenerate(切1minK线)、generateKLine(各周期)、resetThumb、update24HVolume；
 *  3) 状态恢复：initializeThumb/initializeUsdRate（重启后从 MongoDB 恢复）。
 * addHandler(MarketHandler) 体现观察者模式：处理结果分发给多个存储/推送通道。
 */
public interface CoinProcessor {

    void setIsHalt(boolean status);

    void setIsStopKLine(boolean stop);
    
    boolean isStopKline();
    /**
     * 处理新生成的交易信息
     * @param trades
     * @return
     */
    void process(List<ExchangeTrade> trades);

    /**
     * 添加存储器
     * @param storage
     */
    void addHandler(MarketHandler storage);

    CoinThumb getThumb();

    void setMarketService(MarketService service);

    void generateKLine(int range, int field, long time);

    KLine getKLine();

    void initializeThumb();

    void autoGenerate();

    void resetThumb();

    void setExchangeRate(CoinExchangeRate coinExchangeRate);

    void update24HVolume(long time);

    void initializeUsdRate();
}
