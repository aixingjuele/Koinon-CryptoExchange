package com.bizzan.bitrade.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

/**
 * Kafka 消费者配置（撮合引擎侧）
 * 【面试重点·消费端如何保证"不丢、不重、有序"】
 *  1. 不丢：enable.auto.commit=false（配置文件中），关闭自动提交；
 *     Spring Kafka 默认 AckMode.BATCH —— 一批消息处理完后由容器统一提交 offset。
 *     语义 = at-least-once（至少一次）：处理完但提交前宕机会重复消费 → 下游必须幂等。
 *     （对比：auto.commit=true 是"先提交后处理"，宕机丢消息 = at-most-once）
 *  2. 不重：Kafka 消费端本身做不到 exactly-once（除非用 Kafka 事务+幂等生产者组合），
 *     本系统靠业务幂等兜底（DB 状态机、唯一键），见 ExchangeOrderConsumer 注释。
 *  3. 有序：分区内有序 + 一个分区只被一个消费线程消费。
 *     concurrency=9 表示 9 个消费线程，要求 topic 分区数 >= 9 才能充分利用；
 *     若分区数 < 并发数，多余线程空转；若分区数=1，则全局单线程串行（最严格的顺序）。
 *  4. maxPollRecords=50：每批拉 50 条，批量处理减少网络往返，提升吞吐。
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfiguration {

	@Value("${spring.kafka.bootstrap-servers}")
	private String servers;
	@Value("${spring.kafka.consumer.enable.auto.commit}")
	private boolean enableAutoCommit;
	@Value("${spring.kafka.consumer.session.timeout}")
	private String sessionTimeout;
	@Value("${spring.kafka.consumer.auto.commit.interval}")
	private String autoCommitInterval;
	@Value("${spring.kafka.consumer.group.id}")
	private String groupId;
	@Value("${spring.kafka.consumer.auto.offset.reset}")
	private String autoOffsetReset;
	@Value("${spring.kafka.consumer.concurrency}")
	private int concurrency;
	@Value("${spring.kafka.consumer.maxPollRecordsConfig}")
	private int maxPollRecordsConfig;

	public Map<String, Object> consumerConfigs() {
		Map<String, Object> propsMap = new HashMap<>();
		propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
		propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
		propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
		propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
		propsMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecordsConfig);// 每个批次获取数
		return propsMap;
	}

	public ConsumerFactory<String, String> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(concurrency);
		factory.getContainerProperties().setPollTimeout(1500);
		factory.setBatchListener(true);
		return factory;
	}

}
