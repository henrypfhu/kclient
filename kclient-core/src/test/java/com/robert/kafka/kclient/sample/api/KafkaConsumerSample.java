package com.robert.kafka.kclient.sample.api;

import java.io.IOException;

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
		DogHandler mbe = new DogHandler();

		KafkaConsumer kafkaConsumer = new KafkaConsumer(
				"kafka-consumer.properties", "test", 1, mbe);
		try {
			kafkaConsumer.startup();

			try {
				System.in.read();
				System.out.println("Read the exit command.");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			System.out.println("Start to exit...");
			kafkaConsumer.shutdownGracefully();
		}
	}
}
