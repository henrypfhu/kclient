package com.robert.kafka.kclient.sample;

import org.springframework.stereotype.Component;

import com.robert.kafka.kclient.boot.InputConsumer;
import com.robert.kafka.kclient.boot.KafkaHandlers;

@Component
@KafkaHandlers
public class AnnotatedDogHandler {
	@InputConsumer(propertiesFile="kafka-consumer.properties", topic="test", streamNum=1)
	public void dogHandler(Dog dog) {
		System.out.println("Annotated handler receive: " + dog);
	}
}
