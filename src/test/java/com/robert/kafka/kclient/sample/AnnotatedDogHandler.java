package com.robert.kafka.kclient.sample;

import org.springframework.stereotype.Component;

import com.robert.kafka.kclient.boot.InputConsumer;
import com.robert.kafka.kclient.boot.KafkaHandlers;

/**
 * Sample for using annotated message handler.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 */
@Component
@KafkaHandlers
public class AnnotatedDogHandler {
	@InputConsumer(propertiesFile = "kafka-consumer.properties", topic = "test", streamNum = 1)
	public void dogHandler(Dog dog) {
		System.out.println("Annotated handler receive: " + dog);
	}
}
