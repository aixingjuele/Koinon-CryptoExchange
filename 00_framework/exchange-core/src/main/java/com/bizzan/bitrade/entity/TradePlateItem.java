package com.bizzan.bitrade.entity;

import lombok.Data;

import java.math.BigDecimal;

/**
 * 盘口档位（深度数据的一行）。
 *
 * 在撮合流程中的角色：撮合引擎把订单簿（TreeMap）按价格聚合成档位后推送给前端，
 * 形成"买盘/卖盘深度列表"。amount 是该价格档位上所有挂单的数量总和。
 * 注意：盘口是订单簿的"聚合视图"，同一价位可能有多笔挂单，这里只暴露汇总量。
 */
@Data
public class TradePlateItem {
    private BigDecimal price; // 档位价格
    private BigDecimal amount; // 该档位的挂单总量
}
