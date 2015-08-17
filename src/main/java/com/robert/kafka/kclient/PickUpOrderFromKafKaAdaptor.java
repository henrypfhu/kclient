package com.robert.kafka.kclient;

import org.slf4j.Logger;
import org.springframework.stereotype.Service;

@Service
public class PickUpOrderFromKafKaAdaptor {
	private final static Logger logger = null; // = new
												// Logger(PickUpOrderFromKafKaAdaptor.class);

	/**
	 * 读取kafka消息
	 * 
	 * @param executor
	 */
	public void getOrdersFromKafka(PickUpMessageExcutor executor) {

		getOrdersFromKafka(executor, PickUpKafkaConfig.PICKUP_TOPIC_NAME,
				PickUpKafkaConfig.PICKUP_PARTITION_NUM);
	}

	/**
	 * 读取kafka消息
	 * 
	 * @param executor
	 * @param topicName
	 * @param partitionsNum
	 */
	public void getOrdersFromKafka(PickUpMessageExcutor executor,
			String topicName, int partitionsNum) {
		logger.info("new KafkaConsumerClient(kafka_consumer_pickUp.properties, "
				+ executor + ", " + topicName + ", " + partitionsNum + ")");
		KafkaConsumerClient consumer = new KafkaConsumerClient(
				"kafka_consumer_pickUp.properties", executor, topicName,
				partitionsNum);
		logger.info("KafkaConsumerClient has been initialled");
		consumer.start();
		logger.info("KafkaConsumerClient thread start to run ...");
	}

}
