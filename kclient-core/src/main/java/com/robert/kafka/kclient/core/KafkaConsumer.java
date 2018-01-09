package com.robert.kafka.kclient.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
import org.springframework.util.StringUtils;

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

	private String propertiesFile;
	private Properties properties;
	private String topic;
	private int streamNum;

	private MessageHandler handler;

	private ExecutorService streamThreadPool;

	private ExecutorService sharedAsyncThreadPool;

	private ConsumerConnector consumerConnector;

	private List<KafkaStream<String, String>> streams;
	private boolean isAutoCommitOffset = true;

	enum Status {
		INIT, RUNNING, STOPPING, STOPPED;
	};

	private volatile Status status = Status.INIT;

	private int fixedThreadNum = 0;

	private int minThreadNum = 0;
	private int maxThreadNum = 0;

	private boolean isAsyncThreadModel = false;
	private boolean isSharedAsyncThreadPool = false;

	private List<AbstractMessageTask> tasks;

	public KafkaConsumer() {
		// For Spring context
	}

	public KafkaConsumer(String propertiesFile, String topic, int streamNum,
			MessageHandler handler) {
		this(propertiesFile, topic, streamNum, 0, false, handler);
	}

	public KafkaConsumer(String propertiesFile, String topic, int streamNum,
			int fixedThreadNum, MessageHandler handler) {
		this(propertiesFile, topic, streamNum, fixedThreadNum, false, handler);
	}

	public KafkaConsumer(String propertiesFile, String topic, int streamNum,
			int fixedThreadNum, boolean isSharedThreadPool,
			MessageHandler handler) {
		this.propertiesFile = propertiesFile;
		this.topic = topic;
		this.streamNum = streamNum;
		this.fixedThreadNum = fixedThreadNum;
		this.isSharedAsyncThreadPool = isSharedThreadPool;
		this.handler = handler;
		this.isAsyncThreadModel = (fixedThreadNum != 0);

		init();
	}

	public KafkaConsumer(String propertiesFile, String topic, int streamNum,
			int minThreadNum, int maxThreadNum, MessageHandler handler) {
		this(propertiesFile, topic, streamNum, minThreadNum, maxThreadNum,
				false, handler);
	}

	public KafkaConsumer(String propertiesFile, String topic, int streamNum,
			int minThreadNum, int maxThreadNum, boolean isSharedThreadPool,
			MessageHandler handler) {
		this.propertiesFile = propertiesFile;
		this.topic = topic;
		this.streamNum = streamNum;
		this.minThreadNum = minThreadNum;
		this.maxThreadNum = maxThreadNum;
		this.isSharedAsyncThreadPool = isSharedThreadPool;
		this.handler = handler;
		this.isAsyncThreadModel = !(minThreadNum == 0 && maxThreadNum == 0);

		init();
	}

	public KafkaConsumer(Properties properties, String topic, int streamNum,
			MessageHandler handler) {
		this(properties, topic, streamNum, 0, false, handler);
	}

	public KafkaConsumer(Properties properties, String topic, int streamNum,
			int fixedThreadNum, MessageHandler handler) {
		this(properties, topic, streamNum, fixedThreadNum, false, handler);
	}

	public KafkaConsumer(Properties properties, String topic, int streamNum,
			int fixedThreadNum, boolean isSharedThreadPool,
			MessageHandler handler) {
		this.properties = properties;
		this.topic = topic;
		this.streamNum = streamNum;
		this.fixedThreadNum = fixedThreadNum;
		this.isSharedAsyncThreadPool = isSharedThreadPool;
		this.handler = handler;
		this.isAsyncThreadModel = (fixedThreadNum != 0);

		init();
	}

	public KafkaConsumer(Properties properties, String topic, int streamNum,
			int minThreadNum, int maxThreadNum, MessageHandler handler) {
		this(properties, topic, streamNum, minThreadNum, maxThreadNum, false,
				handler);
	}

	public KafkaConsumer(Properties properties, String topic, int streamNum,
			int minThreadNum, int maxThreadNum, boolean isSharedThreadPool,
			MessageHandler handler) {
		this.properties = properties;
		this.topic = topic;
		this.streamNum = streamNum;
		this.minThreadNum = minThreadNum;
		this.maxThreadNum = maxThreadNum;
		this.isSharedAsyncThreadPool = isSharedThreadPool;
		this.handler = handler;
		this.isAsyncThreadModel = !(minThreadNum == 0 && maxThreadNum == 0);

		init();
	}

	public void init() {
		if (properties == null && propertiesFile == null) {
			log.error("The properties object or file can't be null.");
			throw new IllegalArgumentException(
					"The properties object or file can't be null.");
		}

		if (StringUtils.isEmpty(topic)) {
			log.error("The topic can't be empty.");
			throw new IllegalArgumentException("The topic can't be empty.");
		}

		if (isAsyncThreadModel == true && fixedThreadNum <= 0
				&& (minThreadNum <= 0 || maxThreadNum <= 0)) {
			log.error("Either fixedThreadNum or minThreadNum/maxThreadNum is greater than 0.");
			throw new IllegalArgumentException(
					"Either fixedThreadNum or minThreadNum/maxThreadNum is greater than 0.");
		}

		if (isAsyncThreadModel == true && minThreadNum > maxThreadNum) {
			log.error("The minThreadNum should be less than maxThreadNum.");
			throw new IllegalArgumentException(
					"The minThreadNum should be less than maxThreadNum.");
		}

		if (properties == null)
			properties = loadPropertiesfile();

		if (isSharedAsyncThreadPool) {
			sharedAsyncThreadPool = initAsyncThreadPool();
		}

		initGracefullyShutdown();
		initKafka();
	}

	protected Properties loadPropertiesfile() {
		Properties properties = new Properties();
		try {
			properties.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream(propertiesFile));
		} catch (IOException e) {
			log.error("The consumer properties file is not loaded.", e);
			throw new IllegalArgumentException(
					"The consumer properties file is not loaded.", e);
		}

		return properties;
	}

	private ExecutorService initAsyncThreadPool() {
		ExecutorService syncThreadPool = null;
		if (fixedThreadNum > 0)
			syncThreadPool = Executors.newFixedThreadPool(fixedThreadNum);
		else
			syncThreadPool = new ThreadPoolExecutor(minThreadNum, maxThreadNum,
					60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

		return syncThreadPool;
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
		log.info("Auto commit: " + isAutoCommitOffset);

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

		streamThreadPool = Executors.newFixedThreadPool(streamNum);
	}

	public void startup() {
		if (status != Status.INIT) {
			log.error("The client has been started.");
			throw new IllegalStateException("The client has been started.");
		}

		status = Status.RUNNING;

		log.info("Streams num: " + streams.size());
		tasks = new ArrayList<AbstractMessageTask>();
		for (KafkaStream<String, String> stream : streams) {
			AbstractMessageTask abstractMessageTask = (fixedThreadNum == 0 ? new SequentialMessageTask(
					stream, handler) : new ConcurrentMessageTask(stream,
					handler, fixedThreadNum));
			tasks.add(abstractMessageTask);
			streamThreadPool.execute(abstractMessageTask);
		}
	}

	public void shutdownGracefully() {
		status = Status.STOPPING;

		shutdownThreadPool(streamThreadPool, "main-pool");

		if (isSharedAsyncThreadPool)
			shutdownThreadPool(sharedAsyncThreadPool, "shared-async-pool");
		else
			for (AbstractMessageTask task : tasks) {
				task.shutdown();
			}

		if (consumerConnector != null) {
			consumerConnector.shutdown();
		}

		status = Status.STOPPED;
	}

	private void shutdownThreadPool(ExecutorService threadPool, String alias) {
		log.info("Start to shutdown the thead pool: {}", alias);

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

		log.info("Finally shutdown the thead pool: {}", alias);
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

	public MessageHandler getHandler() {
		return handler;
	}

	public void setHandler(MessageHandler handler) {
		this.handler = handler;
	}

	public int getFixedThreadNum() {
		return fixedThreadNum;
	}

	public void setFixedThreadNum(int fixedThreadNum) {
		this.fixedThreadNum = fixedThreadNum;
	}

	public int getMinThreadNum() {
		return minThreadNum;
	}

	public void setMinThreadNum(int minThreadNum) {
		this.minThreadNum = minThreadNum;
	}

	public int getMaxThreadNum() {
		return maxThreadNum;
	}

	public void setMaxThreadNum(int maxThreadNum) {
		this.maxThreadNum = maxThreadNum;
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
					// hasNext() method is implemented by scala, so no checked
					// exception is declared, in addtion, hasNext() may throw
					// Interrupted exception when interrupted, so we have to
					// catch Exception here and then decide if it is interrupted
					// exception
					if (e instanceof InterruptedException) {
						log.info(
								"The worker [Thread ID: {}] has been interrupted when retrieving messages from kafka broker. Maybe the consumer is shutting down.",
								Thread.currentThread().getId());
						log.error("Retrieve Interrupted: ", e);

						if (status != Status.RUNNING) {
							it.clearCurrentChunk();
							shutdown();
							break;
						}
					} else {
						log.error(
								"The worker [Thread ID: {}] encounters an unknown exception when retrieving messages from kafka broker. Now try again.",
								Thread.currentThread().getId());
						log.error("Retrieve Error: ", e);
						continue;
					}
				}

				if (hasNext) {
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

			// Actually it doesn't work, because kafka v0.8 commits once per 30 seconds, so it is bound to consume duplicate messages.
			stream.clear();
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
		private ExecutorService asyncThreadPool;

		ConcurrentMessageTask(KafkaStream<String, String> stream,
				MessageHandler messageHandler, int threadNum) {
			super(stream, messageHandler);

			if (isSharedAsyncThreadPool)
				asyncThreadPool = sharedAsyncThreadPool;
			else {
				asyncThreadPool = initAsyncThreadPool();
			}
		}

		@Override
		protected void handleMessage(final String message) {
			asyncThreadPool.submit(new Runnable() {
				public void run() {
					// if it blows, how to recover
					messageHandler.execute(message);
				}
			});
		}

		protected void shutdown() {
			if (!isSharedAsyncThreadPool)
				shutdownThreadPool(asyncThreadPool, "async-pool-"
						+ Thread.currentThread().getId());
		}
	}
}
