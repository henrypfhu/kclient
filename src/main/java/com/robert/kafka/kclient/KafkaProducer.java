package com.robert.kafka.kclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a producer client which can be used to send the message or the pair
 * of key and message.
 * 
 * It can be used to send one message once or multiple messages once. When
 * multiple messages, it will send only 20 messages in one batch.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
public class KafkaProducer {
	protected static Logger log = LoggerFactory.getLogger(KafkaProducer.class);

	// If the number of one batch is over 20, use 20 instead
	protected static int MULTI_MSG_ONCE_SEND_NUM = 20;

	private Producer<String, String> producer;

	private String defaultTopic;

	private Properties properties;

	public KafkaProducer(String propertiesFile, String defaultTopic) {
		properties = new Properties();
		try {
			properties.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream(propertiesFile));
		} catch (IOException e) {
			log.error("The properties file is not loaded.", e);
			throw new IllegalArgumentException(
					"The properties file is not loaded.", e);
		}
		this.defaultTopic = defaultTopic;

		init();
	}

	public KafkaProducer(Properties properties, String defaultTopic) {
		this.properties = properties;
		this.defaultTopic = defaultTopic;

		init();
	}

	public void init() {
		log.info("Producer properties:" + properties);
		ProducerConfig config = new ProducerConfig(properties);
		producer = new Producer<String, String>(config);
	}

	public void sendWithTopic(String topicName, String message) {
		if (message == null) {
			return;
		}

		if (topicName == null)
			topicName = defaultTopic;

		KeyedMessage<String, String> km = new KeyedMessage<String, String>(
				topicName, message);
		producer.send(km);
	}

	public void sendWithTopic(String topicName, String key, String message) {
		if (message == null) {
			return;
		}

		if (topicName == null)
			topicName = defaultTopic;

		KeyedMessage<String, String> km = new KeyedMessage<String, String>(
				topicName, key, message);
		producer.send(km);
	}

	public void sendWithTopic(String topicName, Collection<String> messages) {
		if (messages == null || messages.isEmpty()) {
			return;
		}

		if (topicName == null)
			topicName = defaultTopic;

		List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();
		int i = 0;
		for (String entry : messages) {
			KeyedMessage<String, String> km = new KeyedMessage<String, String>(
					topicName, entry);
			kms.add(km);
			i++;
			// 一次发送的消息数量超过20的，每20个发送一次
			if (i % MULTI_MSG_ONCE_SEND_NUM == 0) {
				producer.send(kms);
				kms.clear();
			}
		}

		if (!kms.isEmpty()) {
			producer.send(kms);
		}
	}

	public void sendWithTopic(String topicName, Map<String, String> messages) {
		if (messages == null || messages.isEmpty()) {
			return;
		}

		if (topicName == null)
			topicName = defaultTopic;

		List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();

		int i = 0;
		for (Entry<String, String> entry : messages.entrySet()) {
			KeyedMessage<String, String> km = new KeyedMessage<String, String>(
					topicName, entry.getKey(), entry.getValue());
			kms.add(km);
			i++;
			// 一次发送的消息数量超过20（默认值，可修改）的，每20个发送一次
			if (i % MULTI_MSG_ONCE_SEND_NUM == 0) {
				producer.send(kms);
				kms.clear();
			}
		}

		if (!kms.isEmpty()) {
			producer.send(kms);
		}
	}

	public void close() {
		producer.close();
	}
}
