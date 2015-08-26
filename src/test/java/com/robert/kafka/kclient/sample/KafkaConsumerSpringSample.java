package com.robert.kafka.kclient.sample;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.robert.kafka.kclient.core.KafkaConsumer;

/**
 * Sample for use {@link KafkaConsumer} with Spring context.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaConsumerSpringSample {

	public static void main(String[] args) {
		ApplicationContext ac = new ClassPathXmlApplicationContext(
				"kafka-producer.xml");

		KafkaConsumer kafkaConsumer = (KafkaConsumer) ac.getBean("consumer");
		try {
			kafkaConsumer.startup();
		} finally {
			kafkaConsumer.shutdownGracefully();
		}
	}
}
