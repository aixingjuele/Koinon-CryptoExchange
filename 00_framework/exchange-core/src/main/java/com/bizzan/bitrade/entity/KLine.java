package com.bizzan.bitrade.entity;


import lombok.Data;

import java.math.BigDecimal;

/**
 * K线（蜡烛图）数据。
 *
 * 在撮合流程中的角色：market 模块按成交消息实时聚合生成各周期 K 线，
 * 当前周期的 K 线随每笔成交动态更新（开盘价不变、最高/最低/收盘价随成交刷新），
 * 周期结束后固化并开启新一根。前端行情图表的数据来源。
 */
@Data
public class KLine {
    public KLine(){

    }
    public KLine(String period){
        this.period = period;
    }
    private BigDecimal openPrice = BigDecimal.ZERO; // 开盘价（本周期第一笔成交价）
    private BigDecimal highestPrice  = BigDecimal.ZERO; // 最高价
    private BigDecimal lowestPrice  = BigDecimal.ZERO; // 最低价
    private BigDecimal closePrice  = BigDecimal.ZERO; // 收盘价（本周期最新一笔成交价）
    private long time; // 周期起始时间戳
    private String period; // 周期，如 1min/5min/1hour/1day

    /**
     * 成交笔数
     */
    private int count;
    /**
     * 成交量
     */
    private BigDecimal volume = BigDecimal.ZERO;
    /**
     * 成交额
     */
    private BigDecimal turnover = BigDecimal.ZERO;
}
