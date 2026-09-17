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

/*
 * 【面试要点】Kafka 生产者配置（exchange-api 侧，用于发送下单/撤单等消息）。
 * 逐参数说明：
 *  - retries=3：发送失败自动重试，可应对 leader 选举等瞬时故障；
 *    但重试可能产生重复消息（broker 已写入但 ack 丢失），需配合幂等。
 *  - batch.size=256：每个分区攒够 256 字节批量发送，提升吞吐。
 *  - linger.ms=1：发送前最多等 1ms 凑更多消息成批 —— 吞吐与延迟的权衡。
 *  - buffer.memory=1MB：生产者发送缓冲池，消息先进缓冲区再由 Sender 线程批量发出；
 *    缓冲满会阻塞 send()（默认 60s 后抛异常）。
 * 【面试重点】要保证消息"不丢 + 有序"，生产端最佳实践：
 *  1) acks=all：等 ISR 全部副本确认才算成功。本配置未设置，默认 acks=1 只等 leader，
 *     leader 确认后宕机且未同步到 follower 时会丢消息！
 *  2) enable.idempotence=true（Kafka 0.11+ 幂等生产者）：broker 按 PID+序列号自动
 *     去重重试产生的重复消息。
 *  3) max.in.flight.requests.per.connection<=5：开启幂等后，保证乱序重试不会破坏
 *     分区内消息顺序（未开幂等时该值必须为 1 才能严格保序，但吞吐骤降）。
 * 本配置 retries=3 但没开幂等、也没设 acks=all，存在"leader 确认后宕机丢消息"和
 * "重试导致重复消息"两类风险 —— 因此消费端（撮合引擎）必须做幂等处理。
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
		// 注意：自定义分区器被注释掉了，当前使用默认分区策略（有 key 按 key 哈希，无 key 粘性分区）；
		// 若需"同一交易对消息有序"，应以 symbol 作为 key 或启用自定义分区器
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
