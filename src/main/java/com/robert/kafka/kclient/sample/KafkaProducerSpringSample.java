package com.robert.kafka.kclient.sample;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.fastjson.JSON;
import com.robert.kafka.kclient.core.KafkaProducer;

/**
 * Sample for use {@link KafkaProducer} with Spring context.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaProducerSpringSample {
	public static void main(String[] args) throws InterruptedException {
		ApplicationContext ac = new ClassPathXmlApplicationContext(
				"kafka-producer.xml");

		KafkaProducer kafkaProducer = (KafkaProducer) ac.getBean("producer");

		for (int i = 0; i < 100; i++) {
			MyBean myBean = new MyBean();
			myBean.setName("Yours " + i);
			myBean.setId(i);
			kafkaProducer.sendWithTopic("sample-topic",
					JSON.toJSONString(myBean));

			Thread.sleep(100);
		}
	}
}
