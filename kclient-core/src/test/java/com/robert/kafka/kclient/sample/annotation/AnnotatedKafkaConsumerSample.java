package com.robert.kafka.kclient.sample.annotation;

import java.io.IOException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.robert.kafka.kclient.boot.KClientBoot;

/**
 * Sample for use {@link KClientBoot} with annotated message handler.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class AnnotatedKafkaConsumerSample {

	public static void main(String[] args) {
		ApplicationContext ac = new ClassPathXmlApplicationContext(
				"annotated-kafka-consumer.xml");

		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
