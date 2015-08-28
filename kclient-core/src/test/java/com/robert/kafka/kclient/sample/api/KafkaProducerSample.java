package com.robert.kafka.kclient.sample.api;

import com.robert.kafka.kclient.core.KafkaProducer;
import com.robert.kafka.kclient.sample.domain.Dog;

/**
 * Sample for use {@link KafkaProducer} with Java API.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaProducerSample {
	public static void main(String[] args) throws InterruptedException {
		KafkaProducer kafkaProducer = new KafkaProducer(
				"kafka-producer.properties", "test");

		for (int i = 0; i < 10; i++) {
			Dog dog = new Dog();
			dog.setName("Yours " + i);
			dog.setId(i);
			kafkaProducer.sendBean2Topic("test", dog);

			System.out.format("Sending dog: %d \n", i + 1);

			Thread.sleep(100);
		}
	}
}
