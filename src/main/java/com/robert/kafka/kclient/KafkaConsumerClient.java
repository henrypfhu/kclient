package com.robert.kafka.kclient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

import org.apache.log4j.Logger;

public class KafkaConsumerClient {
	private static Logger logger = Logger.getLogger(KafkaConsumerClient.class);
	private String propertyFile = "kafka-consumer.properties";// 配置文件位置
	private String topic;
	private int partitionsNum;

	private MessageExecutor executor; // message listener
	private ExecutorService threadPool;

	private ConsumerConnector consumerConnector;
	private List<KafkaStream<String, String>> partitions;
	protected volatile boolean running = false;
	protected volatile boolean isWorking = false;
	private boolean isAutoCommitOffset = true;

	/**
	 * 
	 * @param propertyFile
	 *            配置文件
	 * @param executor
	 *            消费回调函数
	 * @param topic
	 *            消费的topic
	 * @param partitionsNum
	 *            partition的数量，如果不知道，就写一个你期望的处理线程数，因为设的比partitionsNum小，
	 *            也会消费所有的partitions，设的多了，最多多出来的线程打酱油，我建议最好和partitionsNum相等
	 */
	public KafkaConsumerClient(String propertyFile, MessageExecutor executor,
			String topic, int partitionsNum) {
		this();
		if (propertyFile != null) {
			this.propertyFile = propertyFile;
		}
		this.executor = executor;
		this.topic = topic;
		this.partitionsNum = partitionsNum;

	}

	public KafkaConsumerClient() {
		// 添加钩子，防止重启导致数据还没处理完
		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
				try {
					running = false;
					while (isWorking) {
						Thread.sleep(100);
					}
				} catch (Exception e) {
					logger.error("Thread.sleep occour error", e);
				}
			}
		});
	}

	// init consumer,and start connection and listener
	public void init() throws Exception {
		if (executor == null) {
			throw new RuntimeException("KafkaConsumer,exectuor cant be null!");
		}
		Properties properties = new Properties();
		properties.load(Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(propertyFile));
		logger.info("Consumer properties:" + properties);
		ConsumerConfig config = new ConsumerConfig(properties);
		isAutoCommitOffset = config.autoCommitEnable();
		consumerConnector = Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put(topic, partitionsNum);
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(
				new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> streams = consumerConnector
				.createMessageStreams(topics, keyDecoder, valueDecoder);
		partitions = streams.get(topic);

	}

	public void start() {
		running = true;
		try {
			init();
		} catch (Exception e) {
			logger.error("init error", e);
		}
		if (partitions == null || partitions.isEmpty()) {
			return;
		}
		logger.info("partitions num:" + partitions.size());
		if (threadPool != null) {
			try {
				threadPool.shutdownNow();
			} catch (Exception e) {
				logger.error("threadPool.shutdownNow() error", e);
			}
		}
		threadPool = Executors.newFixedThreadPool(partitionsNum);

		// start
		for (KafkaStream<String, String> partition : partitions) {
			threadPool.execute(new MessageRunner(partition));
		}
	}

	public void close() {
		try {
			// 关闭前保证消费完
			running = false;
			while (isWorking) {
				Thread.sleep(100);
			}
			threadPool.shutdownNow();
		} catch (Exception e) {
			logger.error("close() error", e);
		} finally {
			if (consumerConnector != null) {
				consumerConnector.shutdown();
			}
		}
	}

	public void setLocation(String location) {
		this.propertyFile = location;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartitionsNum() {
		return partitionsNum;
	}

	public void setPartitionsNum(int partitionsNum) {
		this.partitionsNum = partitionsNum;
	}

	public String getLocation() {
		return propertyFile;
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
		return partitions;
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}

	public boolean isWorking() {
		return isWorking;
	}

	public void setWorking(boolean isWorking) {
		this.isWorking = isWorking;
	}

	class MessageRunner implements Runnable {
		private KafkaStream<String, String> partition;

		MessageRunner(KafkaStream<String, String> partition) {
			this.partition = partition;
		}

		public void run() {
			ConsumerIterator<String, String> it = partition.iterator();
			while (running) {
				try {
					if (it.hasNext()) {
						// 手动提交offset,当autocommit.enable=false时使用
						try {
							isWorking = true;
							MessageAndMetadata<String, String> item = it.next();
							logger.info("partiton[" + item.partition()
									+ "] offset[" + item.offset()
									+ "] message[" + item.message() + "]");
							executor.execute(item.message());
							// 如果不是自动提交，就每消费完一个就提交一次offset
							if (!isAutoCommitOffset) {
								consumerConnector.commitOffsets();
							}

						} catch (Exception e) {
							logger.error("single message handle error", e);
						} finally {
							isWorking = false;
						}
					}
				} catch (Exception e) {
					logger.error("message handle error", e);
				}
			}
		}

	}

	public interface MessageExecutor {

		public void execute(String message);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		KafkaConsumerClient consumer = null;
		try {
			MessageExecutor executor = new MessageExecutor() {

				public void execute(String message) {
					System.out.println(Thread.currentThread().getId() + ":"
							+ message);
				}
			};
			consumer = new KafkaConsumerClient(
					"kafka-consumer-demo.properties", executor, "swx", 3);
			consumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
