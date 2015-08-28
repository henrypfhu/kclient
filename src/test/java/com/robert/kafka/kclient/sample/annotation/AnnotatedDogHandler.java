package com.robert.kafka.kclient.sample.annotation;

import com.robert.kafka.kclient.boot.InputConsumer;
import com.robert.kafka.kclient.boot.KafkaHandlers;
import com.robert.kafka.kclient.sample.domain.Dog;

/**
 * Sample for using annotated message handler.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 */

@KafkaHandlers
public class AnnotatedDogHandler {
	@InputConsumer(propertiesFile = "kafka-consumer.properties", topic = "test", streamNum = 1)
	public void dogHandler(Dog dog) {
		System.out.println("Annotated handler receive: " + dog);
	}
}
