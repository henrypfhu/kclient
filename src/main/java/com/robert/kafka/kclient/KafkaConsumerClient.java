package com.robert.kafka.kclient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerClient {
	private static Logger log = LoggerFactory
			.getLogger(KafkaConsumerClient.class);

	private Properties properties;
	private MessageExecutor executor;
	private String topic;
	private int streamNum;

	private ExecutorService threadPool;

	private ConsumerConnector consumerConnector;

	private List<KafkaStream<String, String>> streams;
	private boolean isAutoCommitOffset = true;

	enum Status {
		INIT, RUNNING, STOPPING, STOPPED;
	};

	private volatile Status status = Status.INIT;

	private CountDownLatch exitLatch;

	public KafkaConsumerClient(String propertiesFile, MessageExecutor executor,
			String topic, int streamNum) {
		properties = new Properties();
		try {
			properties.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream(propertiesFile));
		} catch (IOException e) {
			log.error("The properties file is not loaded.", e);
			throw new IllegalArgumentException(
					"The properties file is not loaded.", e);
		}

		this.executor = executor;
		this.topic = topic;
		this.streamNum = streamNum;

		initKafka();
		initGracefullyShutdown();
	}

	public KafkaConsumerClient(Properties properties, MessageExecutor executor,
			String topic, int streamNum) {
		this.properties = properties;

		this.executor = executor;
		this.topic = topic;
		this.streamNum = streamNum;

		initGracefullyShutdown();
		initKafka();
	}

	public void initGracefullyShutdown() {
		// for graceful shutdown
		exitLatch = new CountDownLatch(streamNum);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdownGracefully();
			}
		});
	}

	public void initKafka() {
		if (executor == null) {
			log.error("Exectuor can't be null!");
			throw new RuntimeException("Exectuor can't be null!");
		}

		log.info("Consumer properties:" + properties);
		ConsumerConfig config = new ConsumerConfig(properties);

		isAutoCommitOffset = config.autoCommitEnable();
		consumerConnector = Consumer.createJavaConsumerConnector(config);

		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put(topic, streamNum);
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(
				new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> streamsMap = consumerConnector
				.createMessageStreams(topics, keyDecoder, valueDecoder);

		streams = streamsMap.get(topic);
		log.info("Partitions:" + streams);

		if (streams == null || streams.isEmpty()) {
			log.error("Partions are empty.");
			throw new IllegalArgumentException("Partions are empty.");
		}

		threadPool = Executors.newFixedThreadPool(streamNum);
	}

	public void startup() {
		if (status != Status.INIT) {
			log.error("The client has been started.");
			throw new IllegalStateException("The client has been started.");
		}

		log.info("Streams num: " + streams.size());
		for (KafkaStream<String, String> stream : streams) {
			threadPool.execute(new MessageRunner(stream, executor));
		}

		status = Status.RUNNING;
	}

	private void shutdownGracefully() {
		status = Status.STOPPING;

		boolean suspiciousWakeup = false;

		while (true) {
			try {
				exitLatch.await();
			} catch (InterruptedException e) {
				suspiciousWakeup = true;
			}

			// If not suspicious wakeup, then exit
			if (!suspiciousWakeup)
				break;

			suspiciousWakeup = false;
		}

		// Since the threads stop handling message, just force it to shutdown
		threadPool.shutdownNow();

		if (consumerConnector != null) {
			consumerConnector.shutdown();
		}

		status = Status.STOPPED;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public MessageExecutor getExecutor() {
		return executor;
	}

	public void setExecutor(MessageExecutor executor) {
		this.executor = executor;
	}

	public ConsumerConnector getConsumerConnector() {
		return consumerConnector;
	}

	public List<KafkaStream<String, String>> getPartitions() {
		return streams;
	}

	class MessageRunner implements Runnable {
		private KafkaStream<String, String> partition;

		private MessageExecutor messageExecutor;

		MessageRunner(KafkaStream<String, String> partition,
				MessageExecutor messageExecutor) {
			this.partition = partition;
			this.messageExecutor = messageExecutor;
		}

		public void run() {
			ConsumerIterator<String, String> it = partition.iterator();
			while (status == Status.RUNNING) {
				if (it.hasNext()) {
					// TODO if error, how to handle, continue or throw exception
					MessageAndMetadata<String, String> item = it.next();
					log.debug("partiton[" + item.partition() + "] offset["
							+ item.offset() + "] message[" + item.message()
							+ "]");

					messageExecutor.execute(item.message());

					// if not auto commit, commit it manually
					if (!isAutoCommitOffset) {
						consumerConnector.commitOffsets();
					}
				}
			}
			exitLatch.countDown();
		}
	}
}
