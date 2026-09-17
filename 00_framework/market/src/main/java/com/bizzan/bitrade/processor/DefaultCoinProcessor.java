package com.bizzan.bitrade.processor;


import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.component.CoinExchangeRate;
import com.bizzan.bitrade.entity.CoinThumb;
import com.bizzan.bitrade.entity.ExchangeTrade;
import com.bizzan.bitrade.entity.KLine;
import com.bizzan.bitrade.handler.MarketHandler;
import com.bizzan.bitrade.service.MarketService;

import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 默认交易处理器，产生1mK线信息
 */
/*
 * 【面试要点】行情处理器（每个交易对 symbol 一个实例，由 CoinProcessorFactory 创建）。
 * 职责：根据撮合引擎发来的成交明细(ExchangeTrade)实时聚合出 1 分钟K线(currentKLine)、
 * 今日行情摘要(CoinThumb：开/高/低/收、24h量、涨幅)，再通过 MarketHandler 列表
 * （观察者模式：MongoMarketHandler 存 MongoDB、WebsocketMarketHandler 推 WebSocket、
 * NettyHandler 推 Netty 长连接）分发到存储与推送通道。
 * 并发模型：成交处理 process() 由 Kafka 消费线程驱动，K线切换 autoGenerate() 由
 * @Scheduled 定时线程驱动，二者会并发读写 currentKLine/coinThumb，因此用
 * synchronized(对象) 做细粒度互斥 —— 典型的"单写者 + 定时快照"模式。
 * 缺点：K线/Thumb 状态全在内存，重启后靠 initializeThumb() 从 MongoDB 恢复当日数据，
 * 恢复期间行情有短暂空窗；多实例部署时状态无法共享，只能单节点运行。
 */
@ToString
public class DefaultCoinProcessor implements CoinProcessor {
    private Logger logger = LoggerFactory.getLogger(DefaultCoinProcessor.class);
    private String symbol;
    private String baseCoin;
    private KLine currentKLine;
    private List<MarketHandler> handlers;
    private CoinThumb coinThumb;
    private MarketService service;
    private CoinExchangeRate coinExchangeRate;
    //是否暂时处理
    private Boolean isHalt = true;
    //是否停止K线生成
    private Boolean stopKLine = false;

    public DefaultCoinProcessor(String symbol, String baseCoin) {
        handlers = new ArrayList<>();
        createNewKLine();
        this.baseCoin = baseCoin;
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    // 【面试要点】服务重启后的状态恢复：查询今日已落库的 1min K线，重新累加出 CoinThumb
    // （开高低收、成交量、成交额）。内存态行情组件必须回答"重启如何恢复"，这是常见考点。
    @Override
    public void initializeThumb() {
        Calendar calendar = Calendar.getInstance();
        //将秒、微秒字段置为0
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long nowTime = calendar.getTimeInMillis();
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        long firstTimeOfToday = calendar.getTimeInMillis();
        String period = "1min";
        logger.info("initializeThumb from {} to {}", firstTimeOfToday, nowTime);
        List<KLine> lines = service.findAllKLine(this.symbol, firstTimeOfToday, nowTime, period);
        coinThumb = new CoinThumb();
        synchronized (coinThumb) {
            coinThumb.setSymbol(symbol);
            for (KLine kline : lines) {
                if (kline.getOpenPrice().compareTo(BigDecimal.ZERO) == 0) {
                    continue;
                }
                if (coinThumb.getOpen().compareTo(BigDecimal.ZERO) == 0) {
                    coinThumb.setOpen(kline.getOpenPrice());
                }
                if (coinThumb.getHigh().compareTo(kline.getHighestPrice()) < 0) {
                    coinThumb.setHigh(kline.getHighestPrice());
                }
                if (kline.getLowestPrice().compareTo(BigDecimal.ZERO) > 0 && coinThumb.getLow().compareTo(kline.getLowestPrice()) > 0) {
                    coinThumb.setLow(kline.getLowestPrice());
                }
                if (kline.getClosePrice().compareTo(BigDecimal.ZERO) > 0) {
                    coinThumb.setClose(kline.getClosePrice());
                }
                coinThumb.setVolume(coinThumb.getVolume().add(kline.getVolume()));
                coinThumb.setTurnover(coinThumb.getTurnover().add(kline.getTurnover()));
            }
            coinThumb.setChange(coinThumb.getClose().subtract(coinThumb.getOpen()));
            // 此处计算涨幅并没有以开盘价为标准，而是以最低价
            // 注意：用最低价做分母算涨幅并不严谨（标准做法是 change/开盘价 或 change/昨收），
            // 最低价远低于开盘价时涨幅会被放大，面试时可作为"代码中的不严谨点"指出。
            if (coinThumb.getLow().compareTo(BigDecimal.ZERO) > 0) {
                coinThumb.setChg(coinThumb.getChange().divide(coinThumb.getLow(), 4, RoundingMode.UP));
            }
        }
    }

    // 开启一根新的 1min K线：时间戳对齐到"下一整分钟"（K线时间戳标识其所属周期边界）
    public void createNewKLine() {
        currentKLine = new KLine();
        // 注意：先替换引用再对新对象加锁，构造期尚无并发，此处同步意义有限，属防御性写法
        synchronized (currentKLine) {
            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            //1Min时间要是下一整分钟的
            calendar.add(Calendar.MINUTE, 1);
            currentKLine.setTime(calendar.getTimeInMillis());
            currentKLine.setPeriod("1min");
            currentKLine.setCount(0);
        }
    }

    /**
     * 00:00:00 时重置CoinThumb
     */
    @Override
    public void resetThumb() {
        logger.info("reset coinThumb");
        // 每日 0 点重置今日摘要：把当前 close 记为"昨收"(lastDayClose)，其余字段清零
        synchronized (coinThumb) {
            coinThumb.setOpen(BigDecimal.ZERO);
            coinThumb.setHigh(BigDecimal.ZERO);
            //设置昨收价格
            coinThumb.setLastDayClose(coinThumb.getClose());
            //coinThumb.setClose(BigDecimal.ZERO);
            coinThumb.setLow(BigDecimal.ZERO);
            coinThumb.setChg(BigDecimal.ZERO);
            coinThumb.setChange(BigDecimal.ZERO);
        }
    }

    @Override
    public void setExchangeRate(CoinExchangeRate coinExchangeRate) {
        this.coinExchangeRate = coinExchangeRate;
    }

    // 每分钟定时刷新 24h 成交量：直接查 MongoDB 聚合最近 24h 成交记录，
    // 而不是在内存维护滑动窗口 —— 实现简单，但每次全量聚合，数据量大时有性能压力
    @Override
    public void update24HVolume(long time) {
        if(coinThumb!=null) {
            synchronized (coinThumb) {
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(time);
                calendar.add(Calendar.HOUR_OF_DAY, -24);
                long timeStart = calendar.getTimeInMillis();
                BigDecimal volume = service.findTradeVolume(this.symbol, timeStart, time);
                coinThumb.setVolume(volume.setScale(4, RoundingMode.DOWN));
            }
        }
    }

    @Override
    public void initializeUsdRate() {
        //logger.info("symbol = {} ,baseCoin = {}",this.symbol,this.baseCoin);
        BigDecimal baseUsdRate = coinExchangeRate.getUsdRate(baseCoin);
        coinThumb.setBaseUsdRate(baseUsdRate);
        //logger.info("setBaseUsdRate = ",baseUsdRate);
        BigDecimal multiply = coinThumb.getClose().multiply(coinExchangeRate.getUsdRate(baseCoin));
        //logger.info("setUsdRate = ",multiply);
        coinThumb.setUsdRate(multiply);
    }


    /*
     * 【面试要点】每分钟整点由定时任务(KLineGeneratorJob)调用：把当前K线落库并开启新K线。
     * 与 process() 并发读写 currentKLine，故需 synchronized(currentKLine) 互斥，
     * 保证"切K线"瞬间不会有成交被劈到两根K线之间（聚合与快照的原子性）。
     */
    @Override
    public void autoGenerate() {
        DateFormat df = new SimpleDateFormat("HH:mm:ss");
        //logger.info("auto generate 1min kline in {},data={}", df.format(new Date(currentKLine.getTime())), JSON.toJSONString(currentKLine));
        if(coinThumb != null) {
            synchronized (currentKLine) {
                //没有成交价时存储上一笔成交价
                // 【面试要点】本分钟无成交时用上一笔收盘价填充开高低收 —— 保证K线连续不断档，
                // 这是交易所/行情软件的常见做法（否则前端K线图会出现"空洞"）
                if (currentKLine.getOpenPrice().compareTo(BigDecimal.ZERO) == 0) {
                    currentKLine.setOpenPrice(coinThumb.getClose());
                    currentKLine.setLowestPrice(coinThumb.getClose());
                    currentKLine.setHighestPrice(coinThumb.getClose());
                    currentKLine.setClosePrice(coinThumb.getClose());
                }
                Calendar calendar = Calendar.getInstance();
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                currentKLine.setTime(calendar.getTimeInMillis());
                handleKLineStorage(currentKLine);
                createNewKLine();
            }
        }
    }

    @Override
    public void setIsHalt(boolean status) {
        this.isHalt = status;
    }

    /*
     * 【面试要点】行情处理主入口：Kafka 消费到成交明细(ExchangeTrade)后调用。
     * synchronized(currentKLine) 的作用：K线聚合（本方法）与定时任务 autoGenerate()
     * （每分钟整点切K线）可能并发，必须加锁保护共享的 currentKLine；
     * 锁内串行处理每笔成交：聚合K线 -> 更新今日摘要 -> 分发给各 MarketHandler。
     */
    @Override
    public void process(List<ExchangeTrade> trades) {
        if (!isHalt) {
            if (trades == null || trades.size() == 0) {
                return;
            }
            synchronized (currentKLine) {
                for (ExchangeTrade exchangeTrade : trades) {
                    //处理K线
                    processTrade(currentKLine, exchangeTrade);
                    //处理今日概况信息
                    logger.debug("处理今日概况信息");
                    handleThumb(exchangeTrade);
                    //存储并推送成交信息
                    handleTradeStorage(exchangeTrade);
                }
            }
        }
    }

    // K线聚合核心逻辑：首笔成交初始化开高低收，后续成交只更新 高(max)/低(min)/收(最新价)，
    // 并累加成交笔数、成交量、成交额 —— 标准的 OHLCV 增量聚合
    public void processTrade(KLine kLine, ExchangeTrade exchangeTrade) {
        if (kLine.getClosePrice().compareTo(BigDecimal.ZERO) == 0) {
            //第一次设置K线值
            kLine.setOpenPrice(exchangeTrade.getPrice());
            kLine.setHighestPrice(exchangeTrade.getPrice());
            kLine.setLowestPrice(exchangeTrade.getPrice());
            kLine.setClosePrice(exchangeTrade.getPrice());
        } else {
            kLine.setHighestPrice(exchangeTrade.getPrice().max(kLine.getHighestPrice()));
            kLine.setLowestPrice(exchangeTrade.getPrice().min(kLine.getLowestPrice()));
            kLine.setClosePrice(exchangeTrade.getPrice());
        }
        kLine.setCount(kLine.getCount() + 1);
        kLine.setVolume(kLine.getVolume().add(exchangeTrade.getAmount()));
        BigDecimal turnover = exchangeTrade.getPrice().multiply(exchangeTrade.getAmount());
        kLine.setTurnover(kLine.getTurnover().add(turnover));
    }

    public void handleTradeStorage(ExchangeTrade exchangeTrade) {
        for (MarketHandler storage : handlers) {
            storage.handleTrade(symbol, exchangeTrade, coinThumb);
        }
    }

    public void handleKLineStorage(KLine kLine) {
        for (MarketHandler storage : handlers) {
            storage.handleKLine(symbol, kLine);
        }
    }

    // 更新今日行情摘要 CoinThumb（开高低收/量/额/涨幅/折合USD价格）
    public void handleThumb(ExchangeTrade exchangeTrade) {
        logger.info("handleThumb symbol = {}", this.symbol);
        // synchronized(coinThumb) 与 process() 同理：resetThumb/update24HVolume 等
        // 定时任务与本方法并发读写 coinThumb，需互斥保证摘要字段的整体一致性
        synchronized (coinThumb) {
            if (coinThumb.getOpen().compareTo(BigDecimal.ZERO) == 0) {
                //第一笔交易记为开盘价
                coinThumb.setOpen(exchangeTrade.getPrice());
            }
            coinThumb.setHigh(exchangeTrade.getPrice().max(coinThumb.getHigh()));
            if (coinThumb.getLow().compareTo(BigDecimal.ZERO) == 0) {
                coinThumb.setLow(exchangeTrade.getPrice());
            } else {
                coinThumb.setLow(exchangeTrade.getPrice().min(coinThumb.getLow()));
            }
            coinThumb.setClose(exchangeTrade.getPrice());
            coinThumb.setVolume(coinThumb.getVolume().add(exchangeTrade.getAmount()).setScale(4, RoundingMode.UP));
            BigDecimal turnover = exchangeTrade.getPrice().multiply(exchangeTrade.getAmount()).setScale(4, RoundingMode.UP);
            coinThumb.setTurnover(coinThumb.getTurnover().add(turnover));
            BigDecimal change = coinThumb.getClose().subtract(coinThumb.getOpen());
            coinThumb.setChange(change);
            // 注意：涨幅 chg 同样以最低价(low)为分母，与 initializeThumb() 一样不严谨
            if (coinThumb.getLow().compareTo(BigDecimal.ZERO) > 0) {
                coinThumb.setChg(change.divide(coinThumb.getLow(), 4, BigDecimal.ROUND_UP));
            }
            if ("USDT".equalsIgnoreCase(baseCoin)) {
                logger.info("setUsdRate", exchangeTrade.getPrice());
                coinThumb.setUsdRate(exchangeTrade.getPrice());
            } else {

            }
            coinThumb.setBaseUsdRate(coinExchangeRate.getUsdRate(baseCoin));
            coinThumb.setUsdRate(exchangeTrade.getPrice().multiply(coinExchangeRate.getUsdRate(baseCoin)));
            logger.info("setUsdRate", exchangeTrade.getPrice().multiply(coinExchangeRate.getUsdRate(baseCoin)));
            logger.info("thumb = {}", coinThumb);
        }
    }

    // 注册观察者（MarketHandler）：成交/K线产生后逐一回调，新增分发通道无需改动本类
    @Override
    public void addHandler(MarketHandler storage) {
        handlers.add(storage);
    }


    @Override
    public CoinThumb getThumb() {
        return coinThumb;
    }

    @Override
    public void setMarketService(MarketService service) {
        this.service = service;
    }

    /*
     * 【面试要点】历史K线生成（5min/10min/30min/1h/1day 等）：直接查询该时间段的
     * 成交明细(MongoDB)离线聚合成一根K线；周线/月线则走 processKline() 用日线再聚合，
     * 避免扫描大量成交明细 —— "大周期K线用小周期K线二次聚合"是明显的性能优化点。
     */
    @Override
    public void generateKLine(int range, int field, long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long endTick = calendar.getTimeInMillis();
        String endTime = df.format(calendar.getTime());
        //往前推range个时间单位
        calendar.add(field, -range);
        String fromTime = df.format(calendar.getTime());
        long startTick = calendar.getTimeInMillis();
        System.out.println("time range from " + fromTime + " to " + endTime);


        KLine kLine = new KLine();
        kLine.setTime(endTick);
        String rangeUnit = "";
        if (field == Calendar.MINUTE) {
            rangeUnit = "min";
        } else if (field == Calendar.HOUR_OF_DAY) {
            rangeUnit = "hour";
        } else if (field == Calendar.DAY_OF_WEEK) {
            rangeUnit = "week";
        } else if (field == Calendar.DAY_OF_YEAR) {
            rangeUnit = "day";
        } else if (field == Calendar.DAY_OF_MONTH) {
            rangeUnit = "month";
        }
        kLine.setPeriod(range + rangeUnit);

        List<ExchangeTrade> exchangeTrades = null;
        // 分钟线、日线，直接查询时间周期内的订单成交详情
        if(field == Calendar.MINUTE || field == Calendar.HOUR_OF_DAY || field == Calendar.DAY_OF_YEAR){
            exchangeTrades = service.findTradeByTimeRange(this.symbol, startTick, endTick);
            // 处理K线信息
            for (ExchangeTrade exchangeTrade : exchangeTrades) {
                processTrade(kLine, exchangeTrade);
            }
        }else{ // 周线和月线的处理方法
            processKline(kLine, startTick, endTick, field);
        }

        // 如果开盘价为0，则设置为前一个价格
        if(kLine.getOpenPrice().compareTo(BigDecimal.ZERO) == 0) {
        	kLine.setOpenPrice(coinThumb.getClose());
        	kLine.setClosePrice(coinThumb.getClose());
        	kLine.setLowestPrice(coinThumb.getClose());
        	kLine.setHighestPrice(coinThumb.getClose());
        }
        logger.info("generate " + range + rangeUnit + " kline in {},data={}", df.format(new Date(kLine.getTime())), JSON.toJSONString(kLine));
        service.saveKLine(symbol, kLine);
    }

    // 处理周K线和月K线的更具效率的方法
    // 只查 7/30 条日线做二次聚合（开盘价=首日开盘、收盘价=末日收盘、高低取极值、量额累加），
    // 相比扫描整周/整月的成交明细，IO 量降低几个数量级
    public void processKline(KLine kline, long fromTime, long endTime, int field){
        // 查询过去时间段的日线（7条）
        List<KLine> lines = service.findAllKLine(symbol, fromTime, endTime,"1day");
        if(lines.size() > 0) {
            kline.setOpenPrice(lines.get(0).getOpenPrice()); // 开盘价设置为首日开盘价
            for (KLine item : lines) {
                kline.setHighestPrice(kline.getHighestPrice().max(item.getHighestPrice()));
                kline.setLowestPrice(kline.getLowestPrice().min(item.getLowestPrice()));
                kline.setVolume(kline.getVolume().add(item.getVolume()));
                kline.setTurnover(kline.getTurnover().add(item.getTurnover()));
                kline.setCount(kline.getCount() + item.getCount());
            }
            kline.setClosePrice(lines.get(lines.size() - 1).getClosePrice()); // 收盘价设置为最后一日收盘价
        }
    }

    @Override
    public KLine getKLine() {
        return currentKLine;
    }

	@Override
	public void setIsStopKLine(boolean stop) {
		this.stopKLine = stop;
	}

	@Override
	public boolean isStopKline() {
		return this.stopKLine;
	}
}
