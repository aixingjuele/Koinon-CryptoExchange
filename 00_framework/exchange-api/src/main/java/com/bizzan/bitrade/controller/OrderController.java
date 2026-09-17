package com.bizzan.bitrade.controller;

import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.constant.BooleanEnum;
import com.bizzan.bitrade.constant.MemberLevelEnum;
import com.bizzan.bitrade.constant.SysConstant;
import com.bizzan.bitrade.entity.*;
import com.bizzan.bitrade.entity.transform.AuthMember;
import com.bizzan.bitrade.service.*;
import com.bizzan.bitrade.util.MessageResult;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import static com.bizzan.bitrade.constant.SysConstant.SESSION_MEMBER;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 委托订单处理类（下单/撤单的 HTTP 入口，exchange-api 模块 = 交易网关）
 *
 * 【在整体架构中的位置·面试先讲这个】
 *   用户下单 → 本类做【前置校验 + 资金冻结 + 落库】→ 发 Kafka(exchange-order)
 *   → 撮合引擎(exchange模块)异步撮合 → 用户通过 WebSocket/Netty 收到成交通知
 *
 * 【为什么下单不直接同步撮合？（异步化设计）】
 *   1. 削峰：下单洪峰被 Kafka 缓冲，撮合引擎按自己节奏消费，不会被突发流量打垮
 *   2. 解耦：API 服务可水平扩展（无状态），撮合引擎独立部署（有状态、按交易对拆分）
 *   3. 串行化：Kafka 分区天然把并发下单请求排队，撮合单线程处理（无锁的前提）
 *   代价：用户下单后不能立即知道成交结果，只能拿到 orderId，靠推送/轮询获知成交
 *   —— 这就是"异步撮合"架构，币安/火币等交易所都是这个模式
 *
 * 【资金安全的两道防线】
 *   第一道（本类）：下单时 DB 事务内"冻结资金"（乐观锁SQL，余额不足冻结失败直接拒单）
 *   第二道（market模块）：成交后清算（行锁 for update + 原子SQL 加币扣冻结）
 *   撮合引擎本身【不碰钱】，只算"谁和谁成交了多少" —— 撮合与清算分离，是交易所标准架构
 */
@Slf4j
@RestController
@RequestMapping("/order")
public class OrderController {
    @Autowired
    private ExchangeOrderService orderService;
    @Autowired
    private MemberWalletService walletService;
    @Autowired
    private ExchangeCoinService exchangeCoinService;
    @Autowired
    private CoinService coinService;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ExchangeOrderDetailService exchangeOrderDetailService;
    @Value("${exchange.max-cancel-times:-1}")
    private int maxCancelTimes;
    @Autowired
    private LocaleMessageSourceService msService;
    @Autowired
    private MemberService memberService;
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private RedisTemplate redisTemplate;
    private SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 添加委托订单（下单接口）
     * 【处理流程·面试能背下来】
     *   1. 参数校验：方向/类型/价格>0/数量>0
     *   2. 交易对校验：存在、启用、可交易、买卖方向开关、价格上下限(涨跌停)、市价单开关
     *   3. 精度处理：价格/数量按交易对配置截断(ROUND_DOWN 向下取，保护平台不多给)
     *   4. 数量限额：最小成交量/最大成交量/最小成交额
     *   5. 钱包校验：钱包存在、未锁定
     *   6. 活动模式校验：抢购(QIANGGOU)/分摊(FENTAN) 模式的时间窗与身份限制（IEO 打新玩法）
     *   7. 委托数量限制：单用户同交易对同方向最大挂单数（防恶意刷单堵盘口）
     *   8. addOrder(): 【DB事务】冻结资金(乐观锁) + 订单落库(status=TRADING)
     *   9. 发 Kafka "exchange-order" → 异步撮合
     *
     * 【关键设计：先冻结资金再撮合】
     *   买单冻结 baseCoin(如USDT)：限价单冻结 价格×数量；市价单冻结 全部成交额
     *   卖单冻结 coin(如BTC)：冻结 数量
     *   → 撮合时无需再检查余额，撮合引擎零 DB 访问，这是内存撮合能跑快的前提！
     *
     * 【注意·潜在问题】第8步事务提交与第9步发Kafka不是原子的：
     *   若事务提交后、发Kafka前进程宕机 → 订单在DB是TRADING但撮合器永远收不到
     *   → 资金被永久冻结（需人工或定时任务兜底）。业界解法：本地消息表/事务消息(RocketMQ)/Outbox模式
     * @param authMember
     * @param direction
     * @param symbol
     * @param price
     * @param amount
     * @param type
     *          usedisCount 暂时不用
     * @return
     */
    @RequestMapping("add")
    public MessageResult addOrder(@SessionAttribute(SESSION_MEMBER) AuthMember authMember,
                                    ExchangeOrderDirection direction,String symbol, BigDecimal price,
                                    BigDecimal amount, ExchangeOrderType type) {
//        int expireTime = SysConstant.USER_ADD_EXCHANGE_ORDER_TIME_LIMIT_EXPIRE_TIME;
//        ValueOperations valueOperations =  redisTemplate.opsForValue();
        if(direction == null || type == null){
            return MessageResult.error(500,msService.getMessage("ILLEGAL_ARGUMENT"));
        }
        Member member=memberService.findOne(authMember.getId());

//        if(member.getMemberLevel()== MemberLevelEnum.GENERAL){
//            return MessageResult.error(500,msService.getMessage("REAL_NAME_AUTHENTICATION"));
//        }

        //是否被禁止交易
        if(member.getTransactionStatus().equals(BooleanEnum.IS_FALSE)){
            return MessageResult.error(500,msService.getMessage("CANNOT_TRADE"));
        }
        ExchangeOrder order = new ExchangeOrder();
        //判断限价输入值是否小于零
        if (price.compareTo(BigDecimal.ZERO) <= 0 && type == ExchangeOrderType.LIMIT_PRICE) {
            return MessageResult.error(500, msService.getMessage("EXORBITANT_PRICES"));
        }
        //判断数量小于零
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            return MessageResult.error(500, msService.getMessage("NUMBER_OF_ILLEGAL"));
        }
        //根据交易对名称（symbol）获取交易对儿信息
        ExchangeCoin exchangeCoin = exchangeCoinService.findBySymbol(symbol);
        if (exchangeCoin == null) {
            return MessageResult.error(500, msService.getMessage("NONSUPPORT_COIN"));
        }
        if(exchangeCoin.getEnable() != 1 || exchangeCoin.getExchangeable() != 1) {
        	return MessageResult.error(500, msService.getMessage("COIN_FORBIDDEN"));
        }
        // 不允许卖
        if(exchangeCoin.getEnableSell() == BooleanEnum.IS_FALSE && direction == ExchangeOrderDirection.SELL){
            return MessageResult.error(500, msService.getMessage("STOP_SELLING"));
        }

        // 不允许买
        if(exchangeCoin.getEnableBuy() == BooleanEnum.IS_FALSE && direction == ExchangeOrderDirection.BUY){
            return MessageResult.error(500,  msService.getMessage("STOP_BUYING"));
        }

        //获取基准币
        String baseCoin = exchangeCoin.getBaseSymbol();
        //获取交易币
        String exCoin = exchangeCoin.getCoinSymbol();
        Coin coin;
        //根据交易方向查询币种信息
        if (direction == ExchangeOrderDirection.SELL) {
            coin = coinService.findByUnit(exCoin);
        } else {
            coin = coinService.findByUnit(baseCoin);
        }
        if (coin == null) {
            return MessageResult.error(500, msService.getMessage("NONSUPPORT_COIN"));
        }
        //设置价格精度
        price = price.setScale(exchangeCoin.getBaseCoinScale(), BigDecimal.ROUND_DOWN);
        //委托数量和精度控制
        if (direction == ExchangeOrderDirection.BUY && type == ExchangeOrderType.MARKET_PRICE) {
            amount = amount.setScale(exchangeCoin.getBaseCoinScale(), BigDecimal.ROUND_DOWN);
            //最小成交额控制
            if (amount.compareTo(exchangeCoin.getMinTurnover()) < 0) {
                return MessageResult.error(500, msService.getMessage("LOWEST_TURNOVER") + exchangeCoin.getMinTurnover());
            }
        } else {
            amount = amount.setScale(exchangeCoin.getCoinScale(), BigDecimal.ROUND_DOWN);
            //成交量范围控制
            if(exchangeCoin.getMaxVolume()!=null&&exchangeCoin.getMaxVolume().compareTo(BigDecimal.ZERO)!=0
                    &&exchangeCoin.getMaxVolume().compareTo(amount)<0){
                return MessageResult.error(msService.getMessage("AMOUNT_OVER_SIZE")+" "+exchangeCoin.getMaxVolume());
            }
            if(exchangeCoin.getMinVolume()!=null&&exchangeCoin.getMinVolume().compareTo(BigDecimal.ZERO)!=0
                    &&exchangeCoin.getMinVolume().compareTo(amount)>0){
                return MessageResult.error(msService.getMessage("AMOUNT_TOO_SMALL")+" "+exchangeCoin.getMinVolume());
            }
        }
        if (price.compareTo(BigDecimal.ZERO) <= 0 && type == ExchangeOrderType.LIMIT_PRICE) {
            return MessageResult.error(500, msService.getMessage("EXORBITANT_PRICES"));
        }
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            return MessageResult.error(500, msService.getMessage("NUMBER_OF_ILLEGAL"));
        }
        MemberWallet baseCoinWallet = walletService.findByCoinUnitAndMemberId(baseCoin, member.getId());
        MemberWallet exCoinWallet = walletService.findByCoinUnitAndMemberId(exCoin, member.getId());
        if (baseCoinWallet == null || exCoinWallet == null) {
            return MessageResult.error(500, msService.getMessage("NONSUPPORT_COIN"));
        }
        if (baseCoinWallet.getIsLock() == BooleanEnum.IS_TRUE || exCoinWallet.getIsLock() == BooleanEnum.IS_TRUE) {
            return MessageResult.error(500, msService.getMessage("WALLET_LOCKED"));
        }
        //如果有最低卖价限制，出价不能低于此价,且禁止市场价格卖
        if (direction == ExchangeOrderDirection.SELL && exchangeCoin.getMinSellPrice().compareTo(BigDecimal.ZERO) > 0
                && ((price.compareTo(exchangeCoin.getMinSellPrice()) < 0) || type == ExchangeOrderType.MARKET_PRICE)) {
            return MessageResult.error(500, msService.getMessage("FLOOR_PRICE") + exchangeCoin.getMinSellPrice());
        }
        // 如果有最高买价限制，出价不能高于此价，且禁止市场价格买
        if(direction == ExchangeOrderDirection.BUY && exchangeCoin.getMaxBuyPrice().compareTo(BigDecimal.ZERO) > 0
        		&& ((price.compareTo(exchangeCoin.getMaxBuyPrice()) > 0) || type == ExchangeOrderType.MARKET_PRICE)) {
        	return MessageResult.error(500, msService.getMessage("PRICE_CEILING") + exchangeCoin.getMaxBuyPrice());
        }
        //查看是否启用市价买卖
        if (type == ExchangeOrderType.MARKET_PRICE) {
            if (exchangeCoin.getEnableMarketBuy() == BooleanEnum.IS_FALSE && direction == ExchangeOrderDirection.BUY) {
                return MessageResult.error(500, msService.getMessage("NO_MARKET_PRICE_BUY"));
            } else if (exchangeCoin.getEnableMarketSell() == BooleanEnum.IS_FALSE && direction == ExchangeOrderDirection.SELL) {
                return MessageResult.error(500, msService.getMessage("NO_MARKET_PRICE_SELL"));
            }
        }
        //限制委托数量
        if (exchangeCoin.getMaxTradingOrder() > 0 && orderService.findCurrentTradingCount(member.getId(), symbol, direction) >= exchangeCoin.getMaxTradingOrder()) {
            return MessageResult.error(500, msService.getMessage("MAXIMUM_QUANTITY") + exchangeCoin.getMaxTradingOrder());
        }
        
        // 抢购模式活动订单限制（用户无法在活动前下买单）
        long currentTime = Calendar.getInstance().getTimeInMillis(); // 当前时间戳
        // 抢购模式下，无法在活动开始前下买单，仅限于管理员下卖单
        if(exchangeCoin.getPublishType() == ExchangeCoinPublishType.QIANGGOU) {
        	// 抢购模式订单
        	try {
        		if(currentTime < dateTimeFormat.parse(exchangeCoin.getStartTime()).getTime()) {
        			if(direction == ExchangeOrderDirection.BUY) {
		        		// 抢购未开始
						if(currentTime < dateTimeFormat.parse(exchangeCoin.getStartTime()).getTime()) {
							return MessageResult.error(500, msService.getMessage("ACTIVITY_NOT_STARTED"));
						}
        			}else {
        				// 此处2是管理员用户的ID
            			if(member.getId() != 2) {
    						return MessageResult.error(500, msService.getMessage("UNABLE_TO_PLACE_BUY_ORDER"));
    					}
        			}
        		}else {
        			// 活动进行期间，无法下卖单 + 无法下市价单
        			if(currentTime < dateTimeFormat.parse(exchangeCoin.getEndTime()).getTime()) {
        				if(direction == ExchangeOrderDirection.SELL) {
        					return MessageResult.error(500, msService.getMessage("UNABLE_TO_PLACE_SELL_ORDER"));
        				}
        				if(type == ExchangeOrderType.MARKET_PRICE){
        					return MessageResult.error(500, msService.getMessage("ITS_NOT_MARKETABLE"));
        				}
        			}else {
        				// 清盘期间，无法下单
						if(currentTime < dateTimeFormat.parse(exchangeCoin.getClearTime()).getTime()) {
							return MessageResult.error(500, msService.getMessage("WINDING_UP"));
						}
        			}
        		}
			} catch (ParseException e) {
				e.printStackTrace();
				return MessageResult.error(500,msService.getMessage("EXAPI_UNKNOWN_ERROR0"));
			}
        }
        // 分摊模式活动订单限制(开始前任何人无法下单)
        if(exchangeCoin.getPublishType() == ExchangeCoinPublishType.FENTAN) {
        	try {

				if(currentTime < dateTimeFormat.parse(exchangeCoin.getStartTime()).getTime()) {
					// UNABLE_TO_PLACE_BUY_ORDER
					return MessageResult.error(500, msService.getMessage("ACTIVITY_NOT_STARTED"));
				}else {
					// 活动开始后且在结束前，无法下卖单 + 下单金额必须符合规定
					if(currentTime < dateTimeFormat.parse(exchangeCoin.getEndTime()).getTime()) {
						if(direction == ExchangeOrderDirection.SELL) {
							return MessageResult.error(500, msService.getMessage("ACTIVITY_STARTED_CANT_SELL"));
						}else {
							if(type == ExchangeOrderType.MARKET_PRICE) {
								return MessageResult.error(500, msService.getMessage("ITS_NOT_MARKETABLE"));
							}else {
								if(price.compareTo(exchangeCoin.getPublishPrice()) != 0) {
									return MessageResult.error(500, msService.getMessage("ORDER_PRICE")+exchangeCoin.getPublishPrice());
								}
							}
						}
					}else {
						// 清盘期间，普通用户无法下单，仅有管理员用户ID可下单
						if(currentTime < dateTimeFormat.parse(exchangeCoin.getClearTime()).getTime()) {
							// 此处2和10001是管理员用户的ID
							if(member.getId() != 2 && member.getId() != 10001) {
								return MessageResult.error(500, msService.getMessage("WINDING_UP"));
							}else {
								if(price.compareTo(exchangeCoin.getPublishPrice()) != 0) {
									return MessageResult.error(500, msService.getMessage("ORDER_PRICE")+exchangeCoin.getPublishPrice());
								}
								if(direction == ExchangeOrderDirection.BUY) {
									return MessageResult.error(500, msService.getMessage("PERIOD_LIQUIDATION"));
								}
							}
						}
					}
				}
			} catch (ParseException e) {
				e.printStackTrace();
				return MessageResult.error(500,msService.getMessage("EXAPI_UNKNOWN_ERROR1"));
			}
        }
        order.setMemberId(member.getId());
        order.setSymbol(symbol);
        order.setBaseSymbol(baseCoin);
        order.setCoinSymbol(exCoin);
        order.setType(type);
        order.setDirection(direction);
        if(order.getType() == ExchangeOrderType.MARKET_PRICE){
            order.setPrice(BigDecimal.ZERO);
        }
        else{
            order.setPrice(price);
        }
        order.setUseDiscount("0");
        //限价买入单时amount为用户设置的总成交额
        order.setAmount(amount);

        MessageResult mr = orderService.addOrder(member.getId(), order);
        if (mr.getCode() != 0) {
            return MessageResult.error(500, msService.getMessage("ORDER_FAILED") + mr.getMessage());
        }
        log.info(">>>>>>>>>>订单提交完成>>>>>>>>>>");
        // 发送消息至Exchange系统（异步撮合的入口）
        // 【Kafka顺序性隐患·面试点】此处 send 未指定 key，同一交易对订单可能散落到多个分区导致乱序；
        //   正确做法：kafkaTemplate.send("exchange-order", symbol, json)，symbol 作 key 保证同交易对进同分区
        kafkaTemplate.send("exchange-order", JSON.toJSONString(order));
        MessageResult result = MessageResult.success(msService.getMessage("EXAPI_SUCCESS"));
        result.setData(order.getOrderId());
        return result;
    }


    /**
      * 行情机器人专用：添加委托订单（内部接口，硬编码 sign 校验 + 固定 uid，跳过钱包冻结等部分校验）
      * 【注意】这是给"刷量机器人"开的后门接口：uid 固定为 1/10001（机器人/管理员），
      *   钱包校验被注释掉了 —— 机器人账户可以无资金下单，用于制造盘口深度和成交量（造市）。
      *   面试时可客观提及：小型交易所普遍存在此类造市设计，但正规交易所用专业做市商API+子账户体系。
      * 【安全风险】sign 是硬编码字符串，泄露后任何人可冒用机器人下单。
     * @param uid
     * @param direction
     * @param symbol
     * @param price
     * @param amount
     * @param type
     *          usedisCount 暂时不用
     * @return
     */
    @RequestMapping("mockaddydhdnskd")
    public MessageResult addOrderMock(  Long uid, String sign,
    								ExchangeOrderDirection direction,String symbol, BigDecimal price,
                                    BigDecimal amount, ExchangeOrderType type) {
        //int expireTime = SysConstant.USER_ADD_EXCHANGE_ORDER_TIME_LIMIT_EXPIRE_TIME;
        //ValueOperations valueOperations =  redisTemplate.opsForValue();
        if(direction == null || type == null){
            return MessageResult.error(500,msService.getMessage("ILLEGAL_ARGUMENT"));
        }

//        Member member=memberService.findOne(uid);
//        if(member.getMemberLevel()== MemberLevelEnum.GENERAL){
//            return MessageResult.error(500,msService.getMessage("REAL_NAME_AUTHENTICATION"));
//        }

        if(uid != 1 && uid != 10001) {
        	return MessageResult.error(500,msService.getMessage("ILLEGAL_ARGUMENT"));
        }
        if(!sign.equals("77585211314qazwsx")) {
        	return MessageResult.error(500,msService.getMessage("ILLEGAL_ARGUMENT"));
        }

//        //是否被禁止交易
//        if(member.getTransactionStatus().equals(BooleanEnum.IS_FALSE)){
//            return MessageResult.error(500,msService.getMessage("CANNOT_TRADE"));
//        }

        ExchangeOrder order = new ExchangeOrder();
        //判断限价输入值是否小于零
        if (price.compareTo(BigDecimal.ZERO) <= 0 && type == ExchangeOrderType.LIMIT_PRICE) {
            return MessageResult.error(500, msService.getMessage("EXORBITANT_PRICES"));
        }
        //判断数量小于零
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            return MessageResult.error(500, msService.getMessage("NUMBER_OF_ILLEGAL"));
        }
        //根据交易对名称（symbol）获取交易对儿信息
        ExchangeCoin exchangeCoin = exchangeCoinService.findBySymbol(symbol);
        if (exchangeCoin == null) {
            return MessageResult.error(500, msService.getMessage("NONSUPPORT_COIN"));
        }
        if(exchangeCoin.getEnable() != 1 || exchangeCoin.getExchangeable() != 1) {
        	return MessageResult.error(500, msService.getMessage("COIN_FORBIDDEN"));
        }

        //获取基准币
        String baseCoin = exchangeCoin.getBaseSymbol();
        //获取交易币
        String exCoin = exchangeCoin.getCoinSymbol();
        Coin coin;
        //根据交易方向查询币种信息
        if (direction == ExchangeOrderDirection.SELL) {
            coin = coinService.findByUnit(exCoin);
        } else {
            coin = coinService.findByUnit(baseCoin);
        }
        if (coin == null) {
            return MessageResult.error(500, msService.getMessage("NONSUPPORT_COIN"));
        }
        //设置价格精度
        price = price.setScale(exchangeCoin.getBaseCoinScale(), BigDecimal.ROUND_DOWN);
        //委托数量和精度控制
        if (direction == ExchangeOrderDirection.BUY && type == ExchangeOrderType.MARKET_PRICE) {
            amount = amount.setScale(exchangeCoin.getBaseCoinScale(), BigDecimal.ROUND_DOWN);
            //最小成交额控制
            if (amount.compareTo(exchangeCoin.getMinTurnover()) < 0) {
                return MessageResult.error(500, msService.getMessage("MINIMUM_TURNOVER") + exchangeCoin.getMinTurnover());
            }
        } else {
            amount = amount.setScale(exchangeCoin.getCoinScale(), BigDecimal.ROUND_DOWN);
            //成交量范围控制
            if(exchangeCoin.getMaxVolume()!=null&&exchangeCoin.getMaxVolume().compareTo(BigDecimal.ZERO)!=0
                    &&exchangeCoin.getMaxVolume().compareTo(amount)<0){
                return MessageResult.error(msService.getMessage("AMOUNT_OVER_SIZE")+" "+exchangeCoin.getMaxVolume());
            }
            if(exchangeCoin.getMinVolume()!=null&&exchangeCoin.getMinVolume().compareTo(BigDecimal.ZERO)!=0
                    &&exchangeCoin.getMinVolume().compareTo(amount)>0){
                return MessageResult.error(msService.getMessage("AMOUNT_TOO_SMALL")+" "+exchangeCoin.getMinVolume());
            }
        }
        if (price.compareTo(BigDecimal.ZERO) <= 0 && type == ExchangeOrderType.LIMIT_PRICE) {
            return MessageResult.error(500, msService.getMessage("EXORBITANT_PRICES"));
        }
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            return MessageResult.error(500, msService.getMessage("NUMBER_OF_ILLEGAL"));
        }

//        MemberWallet baseCoinWallet = walletService.findByCoinUnitAndMemberId(baseCoin, member.getId());
//        MemberWallet exCoinWallet = walletService.findByCoinUnitAndMemberId(exCoin, member.getId());
//        if (baseCoinWallet == null || exCoinWallet == null) {
//            return MessageResult.error(500, msService.getMessage("NONSUPPORT_COIN"));
//        }
//        if (baseCoinWallet.getIsLock() == BooleanEnum.IS_TRUE || exCoinWallet.getIsLock() == BooleanEnum.IS_TRUE) {
//            return MessageResult.error(500, msService.getMessage("WALLET_LOCKED"));
//        }

        // 如果有最低卖价限制，出价不能低于此价,且禁止市场价格卖
        if (direction == ExchangeOrderDirection.SELL && exchangeCoin.getMinSellPrice().compareTo(BigDecimal.ZERO) > 0
                && ((price.compareTo(exchangeCoin.getMinSellPrice()) < 0) || type == ExchangeOrderType.MARKET_PRICE)) {
            return MessageResult.error(500, msService.getMessage("EXORBITANT_PRICES"));
        }
        // 如果有最高买价限制，出价不能高于此价，且禁止市场价格买
        if(direction == ExchangeOrderDirection.BUY && exchangeCoin.getMaxBuyPrice().compareTo(BigDecimal.ZERO) > 0
        		&& ((price.compareTo(exchangeCoin.getMaxBuyPrice()) > 0) || type == ExchangeOrderType.MARKET_PRICE)) {
        	return MessageResult.error(500, msService.getMessage("NO_PRICE_CEILING"));
        }
        //查看是否启用市价买卖
        if (type == ExchangeOrderType.MARKET_PRICE) {
            if (exchangeCoin.getEnableMarketBuy() == BooleanEnum.IS_FALSE && direction == ExchangeOrderDirection.BUY) {
                return MessageResult.error(500, msService.getMessage("NO_MARKET_PRICE_BUY"));
            } else if (exchangeCoin.getEnableMarketSell() == BooleanEnum.IS_FALSE && direction == ExchangeOrderDirection.SELL) {
                return MessageResult.error(500, msService.getMessage("NO_MARKET_PRICE_SELL"));
            }
        }

//        //限制委托数量
//        if (exchangeCoin.getMaxTradingOrder() > 0 && orderService.findCurrentTradingCount(uid, symbol, direction) >= exchangeCoin.getMaxTradingOrder()) {
//            return MessageResult.error(500, msService.getMessage("MAXIMUM_QUANTITY") + exchangeCoin.getMaxTradingOrder());
//        }

        // 抢购模式活动订单限制（用户无法在活动前下买单）
        long currentTime = Calendar.getInstance().getTimeInMillis(); // 当前时间戳
        // 抢购模式下，无法在活动开始前下买单，仅限于管理员下卖单
        if(exchangeCoin.getPublishType() == ExchangeCoinPublishType.QIANGGOU) {
        	// 抢购模式订单
        	try {
        		if(currentTime < dateTimeFormat.parse(exchangeCoin.getStartTime()).getTime()) {
        			if(direction == ExchangeOrderDirection.BUY) {
		        		// 抢购未开始
						if(currentTime < dateTimeFormat.parse(exchangeCoin.getStartTime()).getTime()) {
							return MessageResult.error(500, msService.getMessage("ACTIVITY_NOT_STARTED"));
						}
        			}else {
        				// 此处2是管理员用户的ID
            			if(uid != 2 && uid != 1 && uid != 10001) {
    						return MessageResult.error(500, msService.getMessage("UNABLE_TO_PLACE_BUY_ORDER"));
    					}
        			}
        		}else {
        			// 活动进行期间，无法下卖单 + 无法下市价单
        			if(currentTime < dateTimeFormat.parse(exchangeCoin.getEndTime()).getTime()) {
        				if(direction == ExchangeOrderDirection.SELL) {
        					return MessageResult.error(500, msService.getMessage("UNABLE_TO_PLACE_SELL_ORDER"));
        				}
        				if(type == ExchangeOrderType.MARKET_PRICE){
        					return MessageResult.error(500, msService.getMessage("ITS_NOT_MARKETABLE"));
        				}
        			}else {
        				// 清盘期间，无法下单
						if(currentTime < dateTimeFormat.parse(exchangeCoin.getClearTime()).getTime()) {
							return MessageResult.error(500, msService.getMessage("WINDING_UP"));
						}
        			}
        		}
			} catch (ParseException e) {
				e.printStackTrace();
				return MessageResult.error(500,msService.getMessage("EXAPI_UNKNOWN_ERROR0"));
			}
        }
        // 分摊模式活动订单限制(开始前任何人无法下单)
        if(exchangeCoin.getPublishType() == ExchangeCoinPublishType.FENTAN) {
        	try {
        		
				if(currentTime < dateTimeFormat.parse(exchangeCoin.getStartTime()).getTime()) {
					// UNABLE_TO_PLACE_BUY_ORDER
					return MessageResult.error(500, msService.getMessage("ACTIVITY_NOT_STARTED"));
				}else {
					// 活动开始后且在结束前，无法下卖单 + 下单金额必须符合规定
					if(currentTime < dateTimeFormat.parse(exchangeCoin.getEndTime()).getTime()) {
						if(direction == ExchangeOrderDirection.SELL) {
							return MessageResult.error(500, msService.getMessage("ACTIVITY_STARTED_CANT_SELL"));
						}else {
							if(type == ExchangeOrderType.MARKET_PRICE) {
								return MessageResult.error(500, msService.getMessage("ITS_NOT_MARKETABLE"));
							}else {
								if(price.compareTo(exchangeCoin.getPublishPrice()) != 0) {
									return MessageResult.error(500, msService.getMessage("ORDER_PRICE") + exchangeCoin.getPublishPrice());
								}
							}
						}
					}else {
						// 清盘期间，普通用户无法下单，仅有管理员用户ID可下单
						if(currentTime < dateTimeFormat.parse(exchangeCoin.getClearTime()).getTime()) {
							// 此处2是超级管理员用户的ID
							if(uid != 2 && uid != 1 && uid != 10001) {
								return MessageResult.error(500, msService.getMessage("WINDING_UP"));
							}else {
								if(price.compareTo(exchangeCoin.getPublishPrice()) != 0) {
									return MessageResult.error(500, msService.getMessage("ORDER_PRICE") + exchangeCoin.getPublishPrice());
								}
								if(direction == ExchangeOrderDirection.BUY) {
									return MessageResult.error(500, msService.getMessage("PERIOD_LIQUIDATION"));
								}
							}
						}
					}
				}
			} catch (ParseException e) {
				e.printStackTrace();
				return MessageResult.error(500,msService.getMessage("EXAPI_UNKNOWN_ERROR1"));
			}
        }
        order.setMemberId(uid);
        order.setSymbol(symbol);
        order.setBaseSymbol(baseCoin);
        order.setCoinSymbol(exCoin);
        order.setType(type);
        order.setDirection(direction);
        if(order.getType() == ExchangeOrderType.MARKET_PRICE){
            order.setPrice(BigDecimal.ZERO);
        }
        else{
            order.setPrice(price);
        }
        order.setUseDiscount("0");
        //限价买入单时amount为用户设置的总成交额
        order.setAmount(amount);

        MessageResult mr = orderService.addOrder(uid, order);
        if (mr.getCode() != 0) {
            return MessageResult.error(500, msService.getMessage("ORDER_FAILED") + mr.getMessage());
        }
        log.info(">>>>>>>>>>订单提交完成>>>>>>>>>>");
        // 发送消息至Exchange系统
        kafkaTemplate.send("exchange-order", JSON.toJSONString(order));
        MessageResult result = MessageResult.success(msService.getMessage("EXAPI_SUCCESS"));
        result.setData(order.getOrderId());
        return result;
    }
    
    /**
     * 历史委托
     */
    @RequestMapping("history")
    public Page<ExchangeOrder> historyOrder(@SessionAttribute(SESSION_MEMBER) AuthMember member, String symbol, int pageNo, int pageSize) {
        Page<ExchangeOrder> page = orderService.findHistory(member.getId(), symbol, pageNo, pageSize);
        /*
        page.getContent().forEach(exchangeOrder -> {
            //获取交易成交详情
            exchangeOrder.setDetail(exchangeOrderDetailService.findAllByOrderId(exchangeOrder.getOrderId()));
        });
        */
        return page;
    }

    /**
     * 个人中心历史委托
     */
    @RequestMapping("personal/history")
    public Page<ExchangeOrder> personalHistoryOrder(@SessionAttribute(SESSION_MEMBER) AuthMember member,
                                                    @RequestParam(value = "symbol" ,required = false) String symbol,
                                                    @RequestParam(value = "type",required = false) ExchangeOrderType type,
                                                    @RequestParam(value = "status" ,required = false) ExchangeOrderStatus status,
                                                    @RequestParam(value = "startTime",required = false) String startTime,
                                                    @RequestParam(value = "endTime",required = false) String endTime,
                                                    @RequestParam(value = "direction",required = false) ExchangeOrderDirection direction,
                                                    @RequestParam(value = "pageNo",defaultValue = "1") int pageNo,
                                                    @RequestParam(value = "pageSize",defaultValue = "10") int pageSize) {

        Page<ExchangeOrder> page = orderService.findPersonalHistory(member.getId(), symbol, type, status, startTime, endTime,direction, pageNo, pageSize);
        /*
        page.getContent().forEach(exchangeOrder -> {
            //获取交易成交详情
            exchangeOrder.setDetail(exchangeOrderDetailService.findAllByOrderId(exchangeOrder.getOrderId()));
        });
        */
        return page;
    }


    /**
     * 个人中心当前委托
     * @param member
     * @param symbol
     * @param type
     * @param startTime
     * @param endTime
     * @param pageNo
     * @param pageSize
     * @return
     */
    @RequestMapping("personal/current")
    public Page<ExchangeOrder> personalCurrentOrder(@SessionAttribute(SESSION_MEMBER) AuthMember member,
                                                    @RequestParam(value = "symbol",required = false) String symbol,
                                                    @RequestParam(value = "type",required = false) ExchangeOrderType type,
                                                    @RequestParam(value = "startTime",required = false) String startTime,
                                                    @RequestParam(value = "endTime",required = false) String endTime,
                                                    @RequestParam(value = "direction",required = false) ExchangeOrderDirection direction,
                                                    @RequestParam(value = "pageNo",defaultValue = "1") int pageNo,
                                                    @RequestParam(value = "pageSize",defaultValue = "10") int pageSize) {
        Page<ExchangeOrder> page = orderService.findPersonalCurrent(member.getId(), symbol,type,startTime,endTime, direction, pageNo, pageSize);
        page.getContent().forEach(exchangeOrder -> {
            //获取交易成交详情
            BigDecimal tradedAmount = BigDecimal.ZERO;
            BigDecimal turnover = BigDecimal.ZERO;
            List<ExchangeOrderDetail> details = exchangeOrderDetailService.findAllByOrderId(exchangeOrder.getOrderId());
            exchangeOrder.setDetail(details);
            for (ExchangeOrderDetail trade : details) {
                tradedAmount = tradedAmount.add(trade.getAmount());
                turnover = turnover.add(trade.getTurnover());
            }
            exchangeOrder.setTradedAmount(tradedAmount);
            exchangeOrder.setTurnover(turnover);
        });
        return page;
    }

    /**
     * 当前委托
     *
     * @param member
     * @param pageNo
     * @param pageSize
     * @return
     */
    @RequestMapping("current")
    public Page<ExchangeOrder> currentOrder(@SessionAttribute(SESSION_MEMBER) AuthMember member, String symbol, int pageNo, int pageSize) {
        Page<ExchangeOrder> page = orderService.findCurrent(member.getId(), symbol, pageNo, pageSize);
        page.getContent().forEach(exchangeOrder -> {
            //获取交易成交详情
            BigDecimal tradedAmount = BigDecimal.ZERO;
            BigDecimal turnover = BigDecimal.ZERO;
            List<ExchangeOrderDetail> details = exchangeOrderDetailService.findAllByOrderId(exchangeOrder.getOrderId());
            exchangeOrder.setDetail(details);
            for (ExchangeOrderDetail trade : details) {
                tradedAmount = tradedAmount.add(trade.getAmount());
                turnover = turnover.add(trade.getTurnover());
            }
            exchangeOrder.setTradedAmount(tradedAmount);
            exchangeOrder.setTurnover(turnover);
        });
        return page;
    }

    /**
     * 行情机器人专用：当前委托
     * @param uid
     * @param sign
     * @return
     */
    @RequestMapping("mockcurrentydhdnskd")
    public Page<ExchangeOrder> currentOrderMock(Long uid, String sign, String symbol, int pageNo, int pageSize) {
    	if(uid != 1 && uid != 10001) {
        	return null;
        }
        if(!sign.equals("77585211314qazwsx")) {
        	return null;
        }
        Page<ExchangeOrder> page = orderService.findCurrent(uid, symbol, pageNo, pageSize);

//        page.getContent().forEach(exchangeOrder -> {
//            //获取交易成交详情(机器人无需获取详情）
//
//            BigDecimal tradedAmount = BigDecimal.ZERO;
//            List<ExchangeOrderDetail> details = exchangeOrderDetailService.findAllByOrderId(exchangeOrder.getOrderId());
//            exchangeOrder.setDetail(details);
//            for (ExchangeOrderDetail trade : details) {
//                tradedAmount = tradedAmount.add(trade.getAmount());
//            }
//            exchangeOrder.setTradedAmount(tradedAmount);
//
//        });

        return page;
    }
    
    /**
     * 行情机器人专用：交易取消委托
     * @param uid
     * @param orderId
     * @return
     */
    @RequestMapping("mockcancelydhdnskd")
    public MessageResult cancelOrdermock(Long uid, String sign, String orderId) {
        ExchangeOrder order = orderService.findOne(orderId);
        if(uid != 1 && uid != 10001) {
        	return MessageResult.error(500, msService.getMessage("OPERATION_FORBIDDEN"));
        }
        if(!sign.equals("77585211314qazwsx")) {
        	return MessageResult.error(500, msService.getMessage("OPERATION_FORBIDDEN"));
        }
        if (order.getStatus() != ExchangeOrderStatus.TRADING) {
            return MessageResult.error(500, msService.getMessage("ORDER_STATUS_ERROR"));
        }
        // 活动清盘期间，无法撤销订单
        ExchangeCoin exchangeCoin = exchangeCoinService.findBySymbol(order.getSymbol());
        if(exchangeCoin.getPublishType() != ExchangeCoinPublishType.NONE) {
        	long currentTime = Calendar.getInstance().getTimeInMillis(); // 当前时间戳
        	try {
        		// 处在活动结束时间与清盘结束时间之间
				if(currentTime > dateTimeFormat.parse(exchangeCoin.getEndTime()).getTime() &&
				   currentTime < dateTimeFormat.parse(exchangeCoin.getClearTime()).getTime()) {
					return MessageResult.error(500, msService.getMessage("CANNOT_CANCEL_ORDER"));
				}
			} catch (ParseException e) {
				e.printStackTrace();
				return MessageResult.error(500, msService.getMessage("EXAPI_UNKNOWN_ERROR3"));
			}
        }
        if(isExchangeOrderExist(order)){
            // 发送消息至Exchange系统
            kafkaTemplate.send("exchange-order-cancel",JSON.toJSONString(order));
        }
        else{
            //强制取消
            orderService.forceCancelOrder(order);
        }
        return MessageResult.success(msService.getMessage("EXAPI_SUCCESS"));
    }
    
    /**
     * 查询委托成交明细
     *
     * @param member
     * @param orderId
     * @return
     */
    @RequestMapping("detail/{orderId}")
    public List<ExchangeOrderDetail> currentOrder(@SessionAttribute(SESSION_MEMBER) AuthMember member, @PathVariable String orderId) {
        return exchangeOrderDetailService.findAllByOrderId(orderId);
    }

    /**
     * 取消委托（撤单接口）
     * 【撤单双路径·设计细节】
     *   路径A（正常）：isExchangeOrderExist()=true（订单还在撮合器内存中）
     *     → 发 Kafka "exchange-order-cancel" → 撮合器从订单簿移除 → 发 cancel-success
     *     → market 模块消费 → DB 改 CANCELED + 解冻资金
     *   路径B（兜底）：撮合器里找不到该订单（如撮合服务重启后订单簿还没恢复、或订单刚完成）
     *     → forceCancelOrder() 直接在 DB 强制取消并退款
     * 【为什么撤单要先问撮合器？】订单的"真实状态"在撮合器内存里（DB 状态滞后），
     *   直接改 DB 可能把"正在撮合中"的订单撤掉导致资金错乱，所以必须先确认内存状态
     * 【并发时序】撤单请求与撮合并发时，由撮合器内的 synchronized + DB 状态机双重兜底
     *   （详见 CoinTrader.cancelOrder 注释）
     * @param member
     * @param orderId
     * @return
     */
    @RequestMapping("cancel/{orderId}")
    public MessageResult cancelOrder(@SessionAttribute(SESSION_MEMBER) AuthMember member, @PathVariable String orderId) {
        ExchangeOrder order = orderService.findOne(orderId);

        if (order.getMemberId() != member.getId()) {
            return MessageResult.error(500, msService.getMessage("OPERATION_FORBIDDEN"));
        }
        if (order.getStatus() != ExchangeOrderStatus.TRADING) {
            return MessageResult.error(500, msService.getMessage("ORDER_STATUS_ERROR"));
        }
        // 活动清盘期间，无法撤销订单
        ExchangeCoin exchangeCoin = exchangeCoinService.findBySymbol(order.getSymbol());
        if(exchangeCoin.getPublishType() != ExchangeCoinPublishType.NONE) {
        	long currentTime = Calendar.getInstance().getTimeInMillis(); // 当前时间戳
        	try {
        		// 处在活动结束时间与清盘结束时间之间
				if(currentTime > dateTimeFormat.parse(exchangeCoin.getEndTime()).getTime() &&
				   currentTime < dateTimeFormat.parse(exchangeCoin.getClearTime()).getTime()) {
					return MessageResult.error(500, msService.getMessage("CANNOT_CANCEL_ORDER"));
				}
			} catch (ParseException e) {
				e.printStackTrace();
				return MessageResult.error(500, msService.getMessage("EXAPI_UNKNOWN_ERROR3"));
			}
        }
        if(isExchangeOrderExist(order)){
            if (maxCancelTimes > 0 && orderService.findTodayOrderCancelTimes(member.getId(), order.getSymbol()) >= maxCancelTimes) {
                return MessageResult.error(500, msService.getMessage("CANCELLED") + maxCancelTimes + msService.getMessage("SECOND"));
            }
            // 发送消息至Exchange系统
            kafkaTemplate.send("exchange-order-cancel",JSON.toJSONString(order));
        }
        else{
            //强制取消
            orderService.forceCancelOrder(order);
        }
        return MessageResult.success(msService.getMessage("EXAPI_SUCCESS"));
    }

    /**
     * 查找撮合交易器中订单是否存在（通过 Eureka 服务名 + REST 调 exchange 模块的 MonitorController）
     * 【架构观察】这是一次"跨进程查内存状态"的同步调用：
     *   - 撮合器内存状态是唯一权威，DB 只是异步镜像
     *   - 缺点：撤单链路多了一次同步 HTTP，且 exchange 服务不可用时 catch 返回 false
     *     → 会走 forceCancelOrder 强制取消，若此时订单其实正在撮合，可能"边成交边取消"，
     *     最终靠 DB 状态机兜底（tradeCompleted/cancelOrder 都校验 TRADING 状态，先到先赢）
     * @param order
     * @return
     */
    public boolean isExchangeOrderExist(ExchangeOrder order){
        try {
            String serviceName = "SERVICE-EXCHANGE-TRADE";
            String url = "http://" + serviceName + "/monitor/order?symbol=" + order.getSymbol() + "&orderId=" + order.getOrderId() + "&direction=" + order.getDirection() + "&type=" + order.getType();
            ResponseEntity<ExchangeOrder> result = restTemplate.getForEntity(url, ExchangeOrder.class);
            return result != null;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 获取下单时间限制
     * @return
     */
    @GetMapping("/time_limit")
    public MessageResult userAddExchangeTimeLimit(){
        MessageResult mr = new MessageResult();
        mr.setCode(0);
        mr.setMessage("EXAPI_SUCCESS");
        mr.setData(SysConstant.USER_ADD_EXCHANGE_ORDER_TIME_LIMIT_EXPIRE_TIME);
        return mr;
    }
}
