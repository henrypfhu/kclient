package com.robert.kafka.kclient.sample;

import com.robert.kafka.kclient.core.KafkaConsumer;

/**
 * Sample for use {@link KafkaConsumer} with Java API.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaConsumerSample {

	public static void main(String[] args) {
		MyBeanHandler mbe = new MyBeanHandler();

		KafkaConsumer kafkaConsumer = new KafkaConsumer(
				"kafka-client.properties", mbe, "sample-topic", 3);
		try {
			kafkaConsumer.startup();
		} finally {
			kafkaConsumer.shutdownGracefully();
		}
	}
}
