package com.robert.kafka.kclient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

public class KafkaProducerClient {

	private static Logger logger = Logger.getLogger(KafkaProducerClient.class);
	private Producer<String, String> producer;

	// 一次发送的数量超过20的请求按照20分隔发送
	private int multiMsgOnceSendNum = 20;

	private String propertyFile = "kafka-producer.properties";// spring setter

	private String defaultTopic;// spring setter

	public KafkaProducerClient(String location) {
		this(location, null);
	}

	/**
	 * 
	 * @param propertyFile
	 *            配置文件
	 * @param defaultTopic
	 *            默认的发送topic
	 */
	public KafkaProducerClient(String propertyFile, String defaultTopic) {
		if (propertyFile != null) {
			this.propertyFile = propertyFile;
		}
		this.defaultTopic = defaultTopic;
		try {
			init();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public KafkaProducerClient() {

	}

	public void init() throws Exception {
		Properties properties = new Properties();
		properties.load(Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(propertyFile));
		logger.info("Producer properties:" + properties);
		ProducerConfig config = new ProducerConfig(properties);
		producer = new Producer<String, String>(config);
	}

	/**
	 * 
	 * @param message
	 */
	public void sendDefaultTopic(String message) {
		sendWithTopic(defaultTopic, message);
	}

	/**
	 * 
	 * @param key
	 * @param message
	 */
	public void sendDefaultTopic(String key, String message) {
		sendWithTopic(defaultTopic, key, message);
	}

	/**
	 * 
	 * @param messages
	 */
	public void sendDefaultTopic(Collection<String> messages) {
		sendWithTopic(defaultTopic, messages);
	}

	/**
	 * 
	 * @param messages
	 */
	public void sendDefaultTopic(Map<String, String> messages) {
		sendWithTopic(defaultTopic, messages);
	}

	/**
	 * 
	 * @param topicName
	 * @param message
	 */
	public void sendWithTopic(String topicName, String message) {
		if (topicName == null || message == null) {
			return;
		}
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(
				topicName, message);
		producer.send(km);
	}

	/**
	 * 
	 * @param topicName
	 * @param key
	 * @param message
	 */
	public void sendWithTopic(String topicName, String key, String message) {
		if (topicName == null || message == null) {
			return;
		}
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(
				topicName, key, message);
		producer.send(km);
	}

	/**
	 * 
	 * @param topicName
	 * @param messages
	 */
	public void sendWithTopic(String topicName, Collection<String> messages) {
		if (topicName == null || messages == null) {
			return;
		}
		if (messages.isEmpty()) {
			return;
		}
		List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();
		int i = 0;
		for (String entry : messages) {
			KeyedMessage<String, String> km = new KeyedMessage<String, String>(
					topicName, entry);
			kms.add(km);
			i++;
			// 一次发送的消息数量超过20的，每20个发送一次
			if (i % multiMsgOnceSendNum == 0) {
				producer.send(kms);
				kms.clear();
			}
		}

		if (!kms.isEmpty()) {
			producer.send(kms);
		}
	}

	/**
	 * 
	 * @param topicName
	 * @param messages
	 */
	public void sendWithTopic(String topicName, Map<String, String> messages) {
		if (topicName == null || messages == null) {
			return;
		}
		if (messages.isEmpty()) {
			return;
		}
		List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();
		int i = 0;
		for (Entry<String, String> entry : messages.entrySet()) {
			KeyedMessage<String, String> km = new KeyedMessage<String, String>(
					topicName, entry.getKey(), entry.getValue());
			kms.add(km);
			i++;
			// 一次发送的消息数量超过20（默认值，可修改）的，每20个发送一次
			if (i % multiMsgOnceSendNum == 0) {
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

	public int getMultiMsgOnceSendNum() {
		return multiMsgOnceSendNum;
	}

	public void setMultiMsgOnceSendNum(int multiMsgOnceSendNum) {
		this.multiMsgOnceSendNum = multiMsgOnceSendNum;
	}

	public void setLocation(String location) {
		this.propertyFile = location;
	}

	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}

	public Producer<String, String> getProducer() {
		return producer;
	}

	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	public String getLocation() {
		return propertyFile;
	}

	public String getDefaultTopic() {
		return defaultTopic;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		KafkaProducerClient producer = null;
		try {
			producer = new KafkaProducerClient("kafka-producer-demo.properties");
			int i = 0;
			while (true) {
				System.out.println(i);
				producer.sendWithTopic("swx", i + "",
						"this is a sample---------" + i);
				System.out.println("------------" + i);
				i++;
				Thread.sleep(200);

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (producer != null) {
				producer.close();
			}
		}

	}

}
