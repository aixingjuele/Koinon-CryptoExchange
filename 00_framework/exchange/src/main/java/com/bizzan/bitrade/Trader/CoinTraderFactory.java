package com.bizzan.bitrade.Trader;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 撮合器工厂：管理"交易对 → 撮合器实例"的映射
 * 【设计点】用 ConcurrentHashMap 保证多线程下 get/put 安全
 *   （Kafka 多消费线程会并发调用 getTrader，管理后台可能并发 addTrader 上架新交易对）
 * 【部署含义】一个 exchange 服务进程持有【多个】交易对的撮合器（启动时加载所有启用的交易对），
 *   所以"按交易对拆分部署"实际上是：不同节点配置不同的交易对集合（通过 DB 配置 + 节点隔离），
 *   热门交易对(如 BTC/USDT)独占一个节点吃满单机性能，冷门对合并部署共享一个节点。
 *   注意：本系统没有实现"同一交易对多节点热备"，单节点宕机该交易对就停撮合 → 无高可用。
 */
public class CoinTraderFactory {

	private ConcurrentHashMap<String, CoinTrader> traderMap;

	public CoinTraderFactory() {
		traderMap = new ConcurrentHashMap<>();
	}

	//添加，已存在的无法添加（注意：containsKey+put 不是原子的，并发上架同一交易对可能覆盖，
	//更严谨应用 putIfAbsent；实际启动时单线程初始化，无并发问题）
	public void addTrader(String symbol, CoinTrader trader) {
		if(!traderMap.containsKey(symbol)) {
			traderMap.put(symbol, trader);
		}
	}

	//重置，即使已经存在也会覆盖
	public void resetTrader(String symbol, CoinTrader trader) {
		traderMap.put(symbol, trader);
	}
	
	public boolean containsTrader(String symbol) {
		return traderMap.containsKey(symbol);
	}
	
	public CoinTrader getTrader(String symbol) {
		return traderMap.get(symbol);
	}

	public ConcurrentHashMap<String, CoinTrader> getTraderMap() {
		return traderMap;
	}

}
