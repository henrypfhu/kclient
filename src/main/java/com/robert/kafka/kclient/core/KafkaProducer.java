package com.robert.kafka.kclient.core;

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

	private String propertiesFile;
	private Properties properties;

	public KafkaProducer() {
		// For Spring context
	}

	public KafkaProducer(String propertiesFile, String defaultTopic) {
		this.propertiesFile = propertiesFile;
		this.defaultTopic = defaultTopic;

		init();
	}

	public KafkaProducer(Properties properties, String defaultTopic) {
		this.properties = properties;
		this.defaultTopic = defaultTopic;

		init();
	}

	protected void init() {
		if (properties == null) {
			properties = new Properties();
			try {
				properties.load(Thread.currentThread().getContextClassLoader()
						.getResourceAsStream(propertiesFile));
			} catch (IOException e) {
				log.error("The properties file is not loaded.", e);
				throw new IllegalArgumentException(
						"The properties file is not loaded.", e);
			}
		}
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
			// Send the messages 20 at most once
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
			// Send the messages 20 at most once
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

	public String getDefaultTopic() {
		return defaultTopic;
	}

	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}

	public String getPropertiesFile() {
		return propertiesFile;
	}

	public void setPropertiesFile(String propertiesFile) {
		this.propertiesFile = propertiesFile;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}
