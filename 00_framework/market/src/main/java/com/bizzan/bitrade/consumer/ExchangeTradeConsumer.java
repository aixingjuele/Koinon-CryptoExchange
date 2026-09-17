package com.bizzan.bitrade.consumer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.constant.NettyCommand;
import com.bizzan.bitrade.entity.ExchangeOrder;
import com.bizzan.bitrade.entity.ExchangeTrade;
import com.bizzan.bitrade.entity.TradePlate;
import com.bizzan.bitrade.handler.NettyHandler;
import com.bizzan.bitrade.job.ExchangePushJob;
import com.bizzan.bitrade.processor.CoinProcessor;
import com.bizzan.bitrade.processor.CoinProcessorFactory;
import com.bizzan.bitrade.service.ExchangeOrderService;

import lombok.extern.slf4j.Slf4j;

/**
 * 【清算与行情消费端·面试重点】market 模块消费撮合引擎产出的 4 类消息
 *
 * ==================== 消费的 4 个 topic ====================
 *  1. exchange-trade          → 成交明细：清算落库(processExchangeTrade) + K线 + 推送
 *  2. exchange-order-completed → 订单完成：DB 改状态 + 退冻结 + 通知用户
 *  3. exchange-trade-plate     → 盘口变化：缓冲后批量推前端
 *  4. exchange-order-cancel-success → 撤单成功：DB 改状态 + 退冻结 + 通知用户
 *
 * ==================== 线程池并发处理的坑（面试加分项）====================
 *  handleTrade 把每条 Kafka 记录丢进 30~100 线程的线程池并发处理：
 *   - 好处：清算涉及多次 DB 操作（慢），并发提升吞吐
 *   - 代价①【乱序】同一订单的多笔成交可能被不同线程并发处理，
 *     靠 processExchangeTrade 里的 select for update 行锁串行化（DB 层兜底）
 *   - 代价②【重复消费不幂等】processExchangeTrade 无防重，Kafka 重复投递会重复清算
 *     （详见 ExchangeOrderService 类头注释）
 *   - 代价③【队列满拒绝】LinkedBlockingQueue(1024) + AbortPolicy：
 *     成交洪峰时任务堆积超过 1024 会直接抛 RejectedExecutionException → 消息丢失！
 *     （但 offset 还没提交，重启后会重新消费 → 又回到 at-least-once 语义）
 *
 * ==================== 与撮合引擎的分工 ====================
 *  撮合引擎(exchange)：内存算"谁和谁成交多少"，不管钱 → 快
 *  本类(market)：DB 清算"钱怎么动"，不管撮合 → 稳
 *  中间用 Kafka 解耦：撮合不被 DB 拖慢，清算失败可重放 → 经典的 CQRS/读写分离思想
 */
@Component
@Slf4j
public class ExchangeTradeConsumer {
	private Logger logger = LoggerFactory.getLogger(ExchangeTradeConsumer.class);
	@Autowired
	private CoinProcessorFactory coinProcessorFactory;
	@Autowired
	private SimpMessagingTemplate messagingTemplate;
	@Autowired
	private ExchangeOrderService exchangeOrderService;
	@Autowired
	private NettyHandler nettyHandler;
	@Value("${second.referrer.award}")
	private boolean secondReferrerAward;
	//清算线程池：核心30/最大100/队列1024/满则拒绝(AbortPolicy)
	//【注意】拒绝时任务丢失，但 Kafka offset 未提交，重启后重放（at-least-once 兜底）
	private ExecutorService executor = new ThreadPoolExecutor(30, 100, 0L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue<Runnable>(1024), new ThreadPoolExecutor.AbortPolicy());
	@Autowired
	private ExchangePushJob pushJob;

	/**
	 * 处理成交明细（Kafka 批量拉取 → 每条记录丢线程池异步清算）
	 * 【为什么 Kafka 回调里不直接处理？】@KafkaListener 线程是拉取线程，
	 *   若在里同面步做 DB 清算（慢 IO），会阻塞下一批拉取，吞吐被 DB 拖死；
	 *   丢给线程池后拉取线程立即返回 → 拉取与清理解耦（生产者-消费者模式）
	 * @param records
	 */
	@KafkaListener(topics = "exchange-trade", containerFactory = "kafkaListenerContainerFactory")
	public void handleTrade(List<ConsumerRecord<String, String>> records) {
		for (int i = 0; i < records.size(); i++) {
			ConsumerRecord<String, String> record = records.get(i);
			executor.submit(new HandleTradeThread(record));
		}
	}

	/**
	 * 订单完成处理：DB 改状态(COMPLETED) + 退回剩余冻结 + 推送通知用户
	 * 【幂等】tradeCompleted 内部有状态机校验（仅 TRADING 可完结），重复消费安全
	 * 【注意】此方法在 Kafka 拉取线程中同步执行（没走线程池），
	 *   因为订单完成是低频事件（相对成交明细），且必须保证"先清算完再完结"的顺序
	 */
	@KafkaListener(topics = "exchange-order-completed", containerFactory = "kafkaListenerContainerFactory")
	public void handleOrderCompleted(List<ConsumerRecord<String, String>> records) {
		try {
			for (int i = 0; i < records.size(); i++) {
				ConsumerRecord<String, String> record = records.get(i);
				//logger.info("订单交易处理完成消息topic={},value={}", record.topic(), record.value());
				List<ExchangeOrder> orders = JSON.parseArray(record.value(), ExchangeOrder.class);
				for (ExchangeOrder order : orders) {
					String symbol = order.getSymbol();
					// 委托成交完成处理
					exchangeOrderService.tradeCompleted(order.getOrderId(), order.getTradedAmount(),
							order.getTurnover());
					// 推送订单成交
					messagingTemplate.convertAndSend(
							"/topic/market/order-completed/" + symbol + "/" + order.getMemberId(), order);
					nettyHandler.handleOrder(NettyCommand.PUSH_EXCHANGE_ORDER_COMPLETED, order);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 处理模拟交易
	 *
	 * @param records
	 */
	@KafkaListener(topics = "exchange-trade-mocker", containerFactory = "kafkaListenerContainerFactory")
	public void handleMockerTrade(List<ConsumerRecord<String, String>> records) {
		try {
			for (int i = 0; i < records.size(); i++) {
				ConsumerRecord<String, String> record = records.get(i);
				logger.info("mock数据topic={},value={},size={}", record.topic(), record.value(), records.size());
				List<ExchangeTrade> trades = JSON.parseArray(record.value(), ExchangeTrade.class);
				String symbol = trades.get(0).getSymbol();
				// 处理行情
				CoinProcessor coinProcessor = coinProcessorFactory.getProcessor(symbol);
				if (coinProcessor != null) {
					coinProcessor.process(trades);
				}
				pushJob.addTrades(symbol, trades);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 消费交易盘口信息
	 *
	 * @param records
	 */
	@KafkaListener(topics = "exchange-trade-plate", containerFactory = "kafkaListenerContainerFactory")
	public void handleTradePlate(List<ConsumerRecord<String, String>> records) {
		try {
			for (int i = 0; i < records.size(); i++) {
				ConsumerRecord<String, String> record = records.get(i);
				//logger.info("推送盘口信息topic={},value={},size={}", record.topic(), record.value(), records.size());
				TradePlate plate = JSON.parseObject(record.value(), TradePlate.class);
				String symbol = plate.getSymbol();
				pushJob.addPlates(symbol, plate);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 订单取消成功
	 *
	 * @param records
	 */
	@KafkaListener(topics = "exchange-order-cancel-success", containerFactory = "kafkaListenerContainerFactory")
	public void handleOrderCanceled(List<ConsumerRecord<String, String>> records) {
		try {
			for (int i = 0; i < records.size(); i++) {
				ConsumerRecord<String, String> record = records.get(i);
				//logger.info("取消订单消息topic={},value={},size={}", record.topic(), record.value(), records.size());
				ExchangeOrder order = JSON.parseObject(record.value(), ExchangeOrder.class);
				String symbol = order.getSymbol();
				// 调用服务处理
				exchangeOrderService.cancelOrder(order.getOrderId(), order.getTradedAmount(), order.getTurnover());
				// 推送实时成交
				messagingTemplate.convertAndSend("/topic/market/order-canceled/" + symbol + "/" + order.getMemberId(),
						order);
				nettyHandler.handleOrder(NettyCommand.PUSH_EXCHANGE_ORDER_CANCELED, order);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public class HandleTradeThread implements Runnable {
		private ConsumerRecord<String, String> record;

		private HandleTradeThread(ConsumerRecord<String, String> record) {
			this.record = record;
		}

		@Override
		public void run() {
			//logger.info("topic={},value={}", record.topic(), record.value());
			try {
				List<ExchangeTrade> trades = JSON.parseArray(record.value(), ExchangeTrade.class);
				String symbol = trades.get(0).getSymbol();
				CoinProcessor coinProcessor = coinProcessorFactory.getProcessor(symbol);
				for (ExchangeTrade trade : trades) {
					// 成交明细处理（DB 清算：加币/扣冻结/手续费/返佣，内部 for update 行锁 + @Transactional）
					exchangeOrderService.processExchangeTrade(trade, secondReferrerAward);
					// 推送订单成交订阅
					ExchangeOrder buyOrder = exchangeOrderService.findOne(trade.getBuyOrderId());
					ExchangeOrder sellOrder = exchangeOrderService.findOne(trade.getSellOrderId());
					messagingTemplate.convertAndSend(
							"/topic/market/order-trade/" + symbol + "/" + buyOrder.getMemberId(), buyOrder);
					messagingTemplate.convertAndSend(
							"/topic/market/order-trade/" + symbol + "/" + sellOrder.getMemberId(), sellOrder);
					nettyHandler.handleOrder(NettyCommand.PUSH_EXCHANGE_ORDER_TRADE, buyOrder);
					nettyHandler.handleOrder(NettyCommand.PUSH_EXCHANGE_ORDER_TRADE, sellOrder);
				}
				// 处理K线行情（内存聚合当前1分钟K线 + 今日摘要CoinThumb）
				if (coinProcessor != null) {
					coinProcessor.process(trades);
				}
				//成交明细进推送缓冲队列，由 ExchangePushJob 每300ms批量推前端（事件合并降频）
				pushJob.addTrades(symbol, trades);
			} catch (Exception e) {
				//【注意】异常被吞掉只打印堆栈：清算失败的消息不会重试，依赖 Kafka 未提交重放或对账兜底，
				// 生产环境应记录失败消息到死信队列(DLQ)并告警
				e.printStackTrace();
			}
		}
	}
}
