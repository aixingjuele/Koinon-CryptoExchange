┌───────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  撮合引擎 Exchange 模块（单交易对单线程串行）                                 │
│   用户下单 → CoinTrader.trade() → 内存撮合 → 产出 4 类 Kafka 消息（锁外异步发）                                │
└────────────────────────────────────┬──────────────────────────────────────────────────────────────────────┘
                                     │ 4 个 Kafka Topic
         ┌───────────────────────────┼───────────────────────────┬───────────────────────────┐
         ▼                           ▼                           ▼                           ▼
 exchange-trade            exchange-order-completed      exchange-trade-plate        exchange-order-cancel-success
 (成交明细，高频)           (订单完成，低频)                (盘口变化，高频)              (撤单成功，低频)
         │                           │                           │                           │
         ▼                           ▼                           ▼                           ▼
┌───────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              Market 模块消费端 ExchangeTradeConsumer                                          │
│  @KafkaListener 四个方法 (ExchangeTradeConsumer.java:85,99,152,172)                                         │
└────────────┬──────────────────────────────┬───────────────────────────────┬───────────────────────┬────────┘
             │                              │                               │                       │
             ▼ (线程池 30~100并发)           ▼ (同步执行)                    ▼ (同步)                 ▼ (同步)
   HandleTradeThread.run()          handleOrderCompleted()           handleTradePlate()      handleOrderCanceled()
   (ExchangeTradeConsumer:192)      (ExchangeTradeConsumer:100)     (ExchangeTradeConsumer:153) (ExchangeTradeConsumer:173)
             │                              │                               │                       │
             │  每笔成交2件事：              │                               │                       │
             │  ① DB清算扣钱+加币            │  DB订单状态机：                │  进盘口缓冲队列        │  DB订单状态机：
             │  ② 实时聚合行情(K线等)        │  status=TRADING → COMPLETED   │  ExchangePushJob       │  status=TRADING → CANCELED
             │                              │  退剩余冻结资金                │  每300ms批量广播       │  退冻结资金
             │                              │  推送用户订单通知              │  前端(降频防刷屏)       │  推送用户订单通知
             │                              ▼                               ▼                       ▼
             │                    [MySQL: exchange_order 完结]        [WebSocket/Netty 推盘口] [MySQL: exchange_order 撤单]
             │                    [MySQL: member_wallet 解冻]
             │                    [WebSocket/Netty 推用户订单]
             │
             │ ┌────────────────────────────── ② 行情聚合分支 ──────────────────────────────────────┐
             │ │  symbol 路由：CoinProcessorFactory → 每个交易对 1 个 DefaultCoinProcessor 实例       │
             │ │  (DefaultCoinProcessor.java:223 process())                                          │
             │ │                                                                                     │
             │ └────┬───────────────────────────────────────────────────────────────────────────────┘
             │      │ synchronized(currentKLine) 串行处理每笔成交
             │      ▼
             │   分三步 (DefaultCoinProcessor.java:228-238)：
             │     ├─ A. processTrade()       → 增量聚合 1min currentKLine (OHLCV)        DefaultCoinProcessor:244
             │     │                            首笔=开高低收全=price; 后续只更新max high/min low/close + 累加量额
             │     │
             │     ├─ B. handleThumb()         → 增量更新今日摘要 CoinThumb (开/高/低/收/量/额/涨幅)  DefaultCoinProcessor:275
             │     │
             │     └─ C. handleTradeStorage()  → 观察者模式遍历 List<MarketHandler> 分发               DefaultCoinProcessor:262
             │                                    │
             │              ┌─────────────────────┼─────────────────────┐
             │              ▼                     ▼                     ▼
             │     MongoMarketHandler     WebsocketMarketHandler       NettyHandler
             │     (MongoDB落库)           (推WS行情给前端)             (推TCP行情)
             │     handleTrade():24        handleTrade():34            handleTrade():120
             │      │
             │      ▼
             │   mongoTemplate.insert(trade, "exchange_trade_"+symbol)   ← 分表存成交明细
             │
             ▼ ────────────────────── ① DB 资金清算分支 ─────────────────────────────────────────────
             │
             ▼ exchangeOrderService.processExchangeTrade()  (线程池调用，@Transactional + for update 行锁)
             │  核心：1. buy/sell 订单 SELECT ... FOR UPDATE 加行锁串行化
             │        2. 买用户钱包：USDT 冻结 → 扣减（手续费）+ BTC 增加
             │        3. 卖用户钱包：BTC  冻结 → 扣减（手续费）+ USDT 增加
             │        4. 返佣、记录明细流水
             │
             ▼
         [MySQL: member_wallet 余额加减 + exchange_order traded_amount累加 + exchange_operation_log 流水]
             │
             ▼
         推送用户订单 trade 事件 (WebSocket STOMP + Netty)

===========================================================================================================
                        K 线 生 成 的 两 条 路 径（定 时 驱 动 vs 离 线 聚 合）
===========================================================================================================

【路径一：1min K线 实时 + 每分钟整点切K线】（DefaultCoinProcessor + KLineGeneratorJob）

    @Scheduled cron="0 * * * * *" (每分钟第0秒)  —— KLineGeneratorJob:43 handle5minKLine()
                │
                ▼ 遍历所有交易对 CoinProcessor
           processor.autoGenerate()   —— DefaultCoinProcessor:187
                │
                ▼ synchronized(currentKLine) 原子切K线
            ① 本分钟无成交 → 用上一笔 close 填充 OHLC（防止K线断档）DefaultCoinProcessor:195
            ② handleKLineStorage() → 观察者分发给 MongoMarketHandler
                     │
                     ▼ mongoTemplate.insert(kline, "exchange_kline_"+symbol+"_1min")  ← 1min 落库
            ③ createNewKLine() → 时间戳对齐到下一整分钟，开新K线

【路径二：5/10/15/30min / 1h / 1day / 1week / 1month 周期性K线】（定时+离线聚合）

    ┌ 同一次每分钟调度里，用 minute%N==0 的整除关系触发：(KLineGeneratorJob:60-71)
    │     minute%5==0  → processor.generateKLine(5, MINUTE)   → 生成 5min K线
    │     minute%10==0 → generateKLine(10, MINUTE)           → 生成 10min K线
    │     minute%15==0 → generateKLine(15, MINUTE)
    │     minute%30==0 → generateKLine(30, MINUTE)
    │
    ├ @Scheduled cron="0 0 * * * *" 每小时整点 → generateKLine(1, HOUR)  → 1h K线
    │
    └ @Scheduled cron="0 0 0 * * *" 每日0点  → generateKLine(1, DAY_OF_YEAR) → 日K线
                                                     week==1 → 周K线；dayOfMonth==1 → 月K线

                │
                ▼ generateKLine()  DefaultCoinProcessor:336
         ┌──────┴───────────────────────────┐
         ▼ (min/hour/day 级)                ▼ (周线/月线)
   查询 MongoDB 成交明细明细           查询 MongoDB 已有的 1day K线（7条/30条）
   exchange_trade_{symbol}            exchange_kline_{symbol}_1day
   时间范围内逐条 processTrade()       做二次聚合 processKline()（更高效）
   （OHLCV 全量重新聚合）                open=首日开盘，close=末日收盘，高低取极值，量额累加
                │                                     │
                └──────────────┬──────────────────────┘
                               ▼
                  mongoTemplate.insert(kline, "exchange_kline_"+symbol+"_"+period)
                               │
                               ▼
                  MarketHandler 分发 → 推 WebSocket/Netty 通知前端刷新K线图