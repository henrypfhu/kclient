package com.robert.kafka.kclient.sample.annotation;

import com.robert.kafka.kclient.boot.InputConsumer;
import com.robert.kafka.kclient.boot.KafkaHandlers;
import com.robert.kafka.kclient.boot.OutputProducer;
import com.robert.kafka.kclient.sample.domain.Cat;
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
	@OutputProducer(propertiesFile = "kafka-producer.properties", defaultTopic = "test1")
	public Cat dogHandler(Dog dog) {
		System.out.println("Annotated dogHandler handle: " + dog);

		return new Cat(dog);
	}

	@InputConsumer(propertiesFile = "kafka-consumer.properties", topic = "test1", streamNum = 1)
	public void catHandler(Cat cat) {
		System.out.println("Annotated catHandler handle: " + cat);
	}
}
