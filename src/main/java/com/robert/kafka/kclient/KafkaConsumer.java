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

/**
 * This is a consumer client which can be started easily by the startup method
 * and stopped by the shutdownGracefully method.
 * 
 * <p>
 * There are 2 types of MessageRunner internally. One is
 * {@link SequentialMessageRunner} while the other is
 * {@link ConcurrentMessageRunner}. The former uses single thread for a single
 * stream, but the later uses a thread pool for a single stream asynchronously.
 * The former is applied when the business handler is light weight. However, the
 * later is applied when the business handler is heavy weight.
 * 
 * <p>
 * The {@link KafkaConsumer} implements the gracefully shutdown by thread
 * control in which case the thread will finish handling the messages which it
 * is working on although the JVM attempts to exit.
 *
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
public class KafkaConsumer {
	protected static Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

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

	private int threadNum;

	public KafkaConsumer(String propertiesFile, MessageExecutor executor,
			String topic, int streamNum) {
		this(propertiesFile, executor, topic, streamNum, 0);
	}

	public KafkaConsumer(String propertiesFile, MessageExecutor executor,
			String topic, int streamNum, int threadNum) {
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
		this.threadNum = 0;

		initKafka();
		initGracefullyShutdown();
	}

	public KafkaConsumer(Properties properties, MessageExecutor executor,
			String topic, int streamNum) {
		this(properties, executor, topic, streamNum, 0);
	}

	public KafkaConsumer(Properties properties, MessageExecutor executor,
			String topic, int streamNum, int threadNum) {
		this.properties = properties;

		this.executor = executor;
		this.topic = topic;
		this.streamNum = streamNum;
		this.threadNum = threadNum;

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
		log.info("Streams:" + streams);

		if (streams == null || streams.isEmpty()) {
			log.error("Streams are empty.");
			throw new IllegalArgumentException("Streams are empty.");
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
			threadPool.execute(threadNum == 0 ? new SequentialMessageRunner(
					stream, executor) : new ConcurrentMessageRunner(stream,
					executor, threadNum));
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

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public MessageExecutor getExecutor() {
		return executor;
	}

	public void setExecutor(MessageExecutor executor) {
		this.executor = executor;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getStreamNum() {
		return streamNum;
	}

	public void setStreamNum(int streamNum) {
		this.streamNum = streamNum;
	}

	public int getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}

	public Status getStatus() {
		return status;
	}

	class SequentialMessageRunner implements Runnable {
		private KafkaStream<String, String> stream;

		private MessageExecutor messageExecutor;

		SequentialMessageRunner(KafkaStream<String, String> stream,
				MessageExecutor messageExecutor) {
			this.stream = stream;
			this.messageExecutor = messageExecutor;
		}

		public void run() {
			ConsumerIterator<String, String> it = stream.iterator();
			while (status == Status.RUNNING) {
				if (it.hasNext()) {
					// TODO if error, how to handle, continue or throw exception
					MessageAndMetadata<String, String> item = it.next();
					log.debug("partition[" + item.partition() + "] offset["
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

	class ConcurrentMessageRunner implements Runnable {
		private KafkaStream<String, String> stream;

		private MessageExecutor messageExecutor;

		private ExecutorService threadPool;

		ConcurrentMessageRunner(KafkaStream<String, String> stream,
				MessageExecutor messageExecutor, int threadNum) {
			this.stream = stream;
			this.messageExecutor = messageExecutor;

			threadPool = Executors.newFixedThreadPool(threadNum);
		}

		public void run() {
			ConsumerIterator<String, String> it = stream.iterator();
			while (status == Status.RUNNING) {
				if (it.hasNext()) {
					// TODO if error, how to handle, continue or throw exception
					final MessageAndMetadata<String, String> item = it.next();
					log.debug("partition[" + item.partition() + "] offset["
							+ item.offset() + "] message[" + item.message()
							+ "]");

					threadPool.submit(new Runnable() {
						public void run() {
							// if it blows, how to recover
							messageExecutor.execute(item.message());
						}
					});

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
