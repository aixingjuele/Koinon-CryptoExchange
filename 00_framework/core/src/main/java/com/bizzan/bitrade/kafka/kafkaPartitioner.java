package com.bizzan.bitrade.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * 【面试重点】自定义 Kafka 分区器。
 *
 * 背景知识：Kafka 分区器决定一条消息进入 Topic 的哪个分区。Kafka 只保证"同一分区内有序"，
 * 因此业界常用做法是"按业务 key 分区"（如按交易对 symbol 或用户 ID 取模），
 * 让同一 key 的消息进同一分区 → 消费者单线程按序处理 → 实现业务串行化，
 * 这是"用 Kafka 分区实现业务串行化"的关键手段（默认分区策略：有 key 时按 key 的 hash 取模，
 * 无 key 时轮询/粘性分区）。
 *
 * 注意：本项目的实现并没有按 key 分区——exchange 开头的交易相关 Topic 是【随机】分到 1~9 号分区，
 * 其他 Topic 全部进 0 号分区。这意味着：
 *   1. 同一交易对的订单/成交消息可能落到不同分区，Kafka 层面不保证同一交易对的全局顺序；
 *   2. 本项目实际是把"串行化"放在了撮合引擎内部（exchange 模块每个交易对一个 CoinTrader，
 *      内存订单簿单线程撮合），Kafka 分区只起负载均衡作用；
 *   3. 随机分区还可能把消息分到不存在的分区（若 Topic 分区数 < 10），属于潜在隐患。
 * 面试时如被问"如何保证订单按序撮合"，更标准的答案是：按 symbol 哈希分区 + 消费者按序消费。
 */
public class kafkaPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {
		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 交易相关的随机分配，其他分配到0分区
        // 注意：随机分区打破了"同 key 同分区"的有序性前提，且 random() 返回 1~9，
        // 若 Topic 实际分区数不足 10，可能抛出分区不存在异常。
		if(topic.startsWith("exchange")) {
            return random();
        }else {
        	return 0;
        }
	}

	@Override
	public void close() {
		
	}
	
	private static int random() {
		return (int)(Math.random()*9)+1;
	}
}
