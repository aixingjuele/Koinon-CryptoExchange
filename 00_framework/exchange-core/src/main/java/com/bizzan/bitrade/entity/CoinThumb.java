package com.bizzan.bitrade.entity;

import lombok.Data;

import java.math.BigDecimal;

/**
 * 行情摘要（24小时Ticker）。
 *
 * 在撮合流程中的角色：market 模块消费成交消息后实时更新本对象，
 * 供前端"币币交易列表"展示每个交易对的最新价、涨跌幅、24h 量额等。
 * 通常缓存在内存/Redis 中并通过 WebSocket 推送给前端，避免每次请求都聚合成交流水。
 */
@Data
public class CoinThumb {
    private String symbol; // 交易对
    private BigDecimal open = BigDecimal.ZERO; // 24h 开盘价
    private BigDecimal high= BigDecimal.ZERO; // 24h 最高价
    private BigDecimal low= BigDecimal.ZERO; // 24h 最低价
    private BigDecimal close=BigDecimal.ZERO; // 最新成交价
    private BigDecimal chg = BigDecimal.ZERO.setScale(2); // 涨跌幅（百分比，见 DefaultCoinProcessor.handleThumb：change/low 计算）
    private BigDecimal change = BigDecimal.ZERO.setScale(2); // 涨跌额（close - open）
    private BigDecimal volume = BigDecimal.ZERO.setScale(2); // 24h 成交量
    private BigDecimal turnover= BigDecimal.ZERO; // 24h 成交额
    //昨日收盘价
    private BigDecimal lastDayClose = BigDecimal.ZERO; // 涨跌幅的基准价
    //交易币对usd汇率
    private BigDecimal usdRate; // 用于前端折算显示"≈$xx"
    //基币对usd的汇率
    private BigDecimal baseUsdRate;
    // 交易區
    private int zone; // 交易分区（如主区/创新区），前端分 Tab 展示用
}
