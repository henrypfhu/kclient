package com.robert.kafka.kclient.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

import com.robert.kafka.kclient.handlers.MessageHandler;

/**
 * This is a consumer client which can be started easily by the startup method
 * and stopped by the shutdownGracefully method.
 * 
 * <p>
 * There are 2 types of MessageRunner internally. One is
 * {@link SequentialMessageTask} while the other is
 * {@link ConcurrentMessageTask}. The former uses single thread for a single
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
	private MessageHandler handler;
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

	private int threadNum;

	public KafkaConsumer() {
		// For Spring context
	}

	public KafkaConsumer(String propertiesFile, MessageHandler handler,
			String topic, int streamNum) {
		this(propertiesFile, handler, topic, streamNum, 0);
	}

	public KafkaConsumer(String propertiesFile, MessageHandler handler,
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

		this.handler = handler;
		this.topic = topic;
		this.streamNum = streamNum;
		this.threadNum = 0;

		init();
	}

	public KafkaConsumer(Properties properties, MessageHandler handler,
			String topic, int streamNum) {
		this(properties, handler, topic, streamNum, 0);
	}

	public KafkaConsumer(Properties properties, MessageHandler handler,
			String topic, int streamNum, int threadNum) {
		this.properties = properties;

		this.handler = handler;
		this.topic = topic;
		this.streamNum = streamNum;
		this.threadNum = threadNum;

		init();
	}

	public void init() {
		initGracefullyShutdown();
		initKafka();
	}

	protected void initGracefullyShutdown() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdownGracefully();
			}
		});
	}

	protected void initKafka() {
		if (handler == null) {
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
			threadPool.execute(threadNum == 0 ? new SequentialMessageTask(
					stream, handler) : new ConcurrentMessageTask(stream,
					handler, threadNum));
		}

		status = Status.RUNNING;
	}

	public void shutdownGracefully() {
		status = Status.STOPPING;

		shutdownThreadPool(threadPool, "main-pool");

		if (consumerConnector != null) {
			consumerConnector.shutdown();
		}

		status = Status.STOPPED;
	}

	private void shutdownThreadPool(ExecutorService threadPool, String alias) {
		log.info("Start to shutdown the thead pool: %s", alias);

		threadPool.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
				threadPool.shutdownNow(); // Cancel currently executing tasks
				log.warn("Interrupt the worker, which may cause some task inconsistent. Please check the biz logs.");

				// Wait a while for tasks to respond to being cancelled
				if (!threadPool.awaitTermination(60, TimeUnit.SECONDS))
					log.error("Thread pool can't be shutdown even with interrupting worker threads, which may cause some task inconsistent. Please check the biz logs.");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			threadPool.shutdownNow();
			log.error("The current server thread is interrupted when it is trying to stop the worker threads. This may leave an inconcistent state. Please check the biz logs.");

			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}

		log.info("Finally shutdown the thead pool: %s", alias);
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public MessageHandler getHandler() {
		return handler;
	}

	public void setHandler(MessageHandler handler) {
		this.handler = handler;
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

	abstract class AbstractMessageTask implements Runnable {
		protected KafkaStream<String, String> stream;

		protected MessageHandler messageHandler;

		AbstractMessageTask(KafkaStream<String, String> stream,
				MessageHandler messageHandler) {
			this.stream = stream;
			this.messageHandler = messageHandler;
		}

		public void run() {
			ConsumerIterator<String, String> it = stream.iterator();
			while (status == Status.RUNNING) {
				boolean hasNext = false;
				try {
					hasNext = it.hasNext();
				} catch (Exception e) {
					// hasNext method is implemented by scala, so no checked
					// exception is declared, we have to catch Exception here
					if (e instanceof InterruptedException) {
						log.info(
								"The worker [Thread ID: %d] has been interrupted when retrieving messages from kafka broker. Maybe the consumer is shutting down.",
								Thread.currentThread().getId());
						break;
					} else {
						log.error(
								"The worker [Thread ID: %d] encounters an unknown exception when retrieving messages from kafka broker. Now try again.",
								Thread.currentThread().getId());
						continue;
					}
				}

				if (hasNext) {
					// TODO if error, how to handle, continue or throw exception
					MessageAndMetadata<String, String> item = it.next();
					log.debug("partition[" + item.partition() + "] offset["
							+ item.offset() + "] message[" + item.message()
							+ "]");

					handleMessage(item.message());

					// if not auto commit, commit it manually
					if (!isAutoCommitOffset) {
						consumerConnector.commitOffsets();
					}
				}
			}
		}

		protected void shutdown() {
			// Pleaceholder
		}

		protected abstract void handleMessage(String message);
	}

	class SequentialMessageTask extends AbstractMessageTask {
		SequentialMessageTask(KafkaStream<String, String> stream,
				MessageHandler messageHandler) {
			super(stream, messageHandler);
		}

		@Override
		protected void handleMessage(String message) {
			messageHandler.execute(message);
		}
	}

	class ConcurrentMessageTask extends AbstractMessageTask {
		private ExecutorService threadPool;

		ConcurrentMessageTask(KafkaStream<String, String> stream,
				MessageHandler messageHandler, int threadNum) {
			super(stream, messageHandler);

			threadPool = Executors.newFixedThreadPool(threadNum);
		}

		@Override
		protected void handleMessage(final String message) {
			threadPool.submit(new Runnable() {
				public void run() {
					// if it blows, how to recover
					messageHandler.execute(message);
				}
			});
		}

		protected void shutdown() {
			shutdownThreadPool(threadPool, "stream-pool-"
					+ Thread.currentThread().getId());
		}
	}
}
