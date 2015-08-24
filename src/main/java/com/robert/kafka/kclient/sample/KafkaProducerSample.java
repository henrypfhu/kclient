package com.robert.kafka.kclient.sample;

import com.alibaba.fastjson.JSON;
import com.robert.kafka.kclient.core.KafkaProducer;

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
				"kafka-client.properties", "sample-topic");

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
