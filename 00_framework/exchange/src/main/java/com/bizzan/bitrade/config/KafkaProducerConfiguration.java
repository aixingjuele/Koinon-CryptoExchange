package com.bizzan.bitrade.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Kafka 生产者配置（撮合引擎侧：发送成交明细/订单完成/盘口变化）
 * 【面试重点·发送端如何保证不丢、有序、幂等】
 *  本配置项：retries=3, batch.size=256, linger=1ms, buffer.memory=1MB
 *  【缺失的关键配置·面试加分项】
 *   1. acks 未设置（默认 acks=1）：只等 leader 确认就返回成功，
 *      leader 宕机且副本未同步时消息丢失！金融场景应设 acks=all（等全部 ISR 副本确认）
 *   2. enable.idempotence 未开启：Kafka 0.11+ 支持幂等生产者（PID+序列号去重），
 *      开启后 retries 导致的重复发送会被 broker 自动去重，单分区 exactly-once
 *   3. max.in.flight.requests.per.connection 未设置：默认 5，若未开幂等且 retries>0，
 *      第一批失败重试时第二批可能先成功 → 分区内乱序！
 *      解决：开幂等（自动保证顺序）或设该参数=1（牺牲吞吐换严格顺序）
 *  【batch.size + linger 的权衡】批量攒消息提升吞吐，但增加毫秒级延迟；
 *    撮合结果推送对延迟敏感，所以 linger 只设 1ms（低延迟优先）
 */
@Configuration
@EnableKafka
public class KafkaProducerConfiguration {

	@Value("${spring.kafka.bootstrap-servers}")
	private String servers;
	@Value("${spring.kafka.producer.retries}")
	private int retries;
	@Value("${spring.kafka.producer.batch.size}")
	private int batchSize;
	@Value("${spring.kafka.producer.linger}")
	private int linger;
	@Value("${spring.kafka.producer.buffer.memory}")
	private int bufferMemory;

	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		props.put(ProducerConfig.RETRIES_CONFIG, retries);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
		props.put(ProducerConfig.LINGER_MS_CONFIG, linger);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
//		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.bizzan.bitrade.kafka.kafkaPartitioner");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return props;
	}

	public ProducerFactory<String, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<String, String>(producerFactory());
	}

}
