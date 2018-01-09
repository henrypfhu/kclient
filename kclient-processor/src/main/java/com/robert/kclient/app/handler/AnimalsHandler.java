package com.robert.kclient.app.handler;

import java.io.IOException;

import com.robert.kafka.kclient.boot.ErrorHandler;
import com.robert.kafka.kclient.boot.InputConsumer;
import com.robert.kafka.kclient.boot.KafkaHandlers;
import com.robert.kafka.kclient.boot.OutputProducer;
import com.robert.kclient.app.domain.Cat;
import com.robert.kclient.app.domain.Dog;

/**
 * Sample for using annotated message handler.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 */

@KafkaHandlers
public class AnimalsHandler {
	@InputConsumer(propertiesFile = "kafka-consumer.properties", topic = "test", streamNum = 1)
	@OutputProducer(propertiesFile = "kafka-producer.properties", defaultTopic = "test1")
	public Cat dogHandler(Dog dog) {
		System.out.println("Annotated dogHandler handles: " + dog);

		return new Cat(dog);
	}


	@InputConsumer(propertiesFile = "kafka-consumer.properties", topic = "test1", streamNum = 1)
	public void catHandler(Cat cat) throws IOException {
		System.out.println("Annotated catHandler handles: " + cat);

		throw new IOException("Man made exception.");
	}

	@ErrorHandler(exception = IOException.class, topic = "test1")
	public void ioExceptionHandler(IOException e, String message) {
		System.out.println("Annotated excepHandler handles: " + e);
	}
}
