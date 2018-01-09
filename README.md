# Kafka客户端(KClient)

KClient是一个简单易用，有效集成，高性能，高稳定的Kafka Java客户端。

此文档包含了背景介绍、功能特性、使用指南、API简介、后台监控和管理、消息处理机模板项目、架构设计以及性能压测相关章节。如果你想使用KClient快速的构建Kafka处理机服务，请参考消息处理机模板项目章节; 如果你想了解KClient的其他使用方式、功能特性、监控和管理等，请参考背景介绍、功能恶性、使用指南、API简介、后台监控和管理等章节; 如果你想更深入的理解KClient的架构设计和性能Benchmark，请参考架构设计和性能压测章节。

## 背景介绍

消息队列在互联网领域里得到了广泛的应用，它多应用在异步处理，模块之间的解偶和高并发的消峰等场景，消息队列中表现最好的当属Apache开源项目Kafka，Kafka使用支持高并发的Scala语言开发，利用操作系统的缓存原理达到高性能，并且天生具有可分区，分布式的特点，而且有不同语言的客户端，使用起来非常的方便。KClient提供了一个简单易用，有效集成，高性能，高稳定的Kafka Java客户端。

在继续介绍KClient的功能特性，使用方法和架构设计之前,读者需要对Kafka进行基本的学习和了解。如果你是Kafka的初学者或者从未接触过Kafka，请参考我的博客文章：[Kafka的那些事儿](http://cloudate.net/?p=1763)。如果你英文还不错，也可以直接参考Kafka官方在线文档：[Kafka 0.8.2 Documentation](kafka.apache.org/documentation.html)。

## 功能特性

**1.简单易用**

简化了Kafka客户端API的使用方法, 特别是对消费端开发，消费端开发者只需要实现MessageHandler接口或者相关子类，在实现中处理消息完成业务逻辑，并且在主线程中启动封装的消费端服务器即可。它提供了各种常用的MessageHandler，框架自动转换消息到领域对象模型或者JSON对象等数据结构，让开发者更专注于业务处理。如果使用服务源码注解的方式声明消息处理机的后台，可以将一个通用的服务方法直接转变成具有完善功能的处理Kafka消息队列的处理机，使用起来极其简单，代码看起来一目了然，在框架级别通过多种线程池技术保证了处理机的高性能。

在使用方面，它提供了多种使用方式：1. 直接使用Java API; 2. 与Spring环境无缝集成; 3. 服务源码注解，通过注解声明方式启动Kafka消息队列的处理机。除此之外，它基于注解提供了消息处理机的模板项目，可以根据模板项目通过配置快速开发Kafka的消息处理机。
 
**2.高性能**

为了在不同的业务场景下实现高性能, 它提供不同的线程模型： 1. 适合轻量级服务的同步线程模型; 2. 适合IO密集型服务的异步线程模型（细分为所有消费者流共享线程池和每个流独享线程池）。另外，异步模型中的线程池也支持确定数量线程的线程池和线程数量可伸缩的线程池。

**3.高稳定性**

框架级别处理了通用的异常，计入错误日志，可用于错误手工恢复或者洗数据，并实现了优雅关机和重启等功能。

## 使用指南

KClient提供了三种使用方法，对于每一种方法，按照下面的步骤可快速构建Kafka生产者和消费者程序。

**前置步骤**

1).下载源代码后在项目根目录执行如下命令安装打包文件到你的Maven本地库。

> mvn install

2).在你的项目pom.xml文件中添加对KClient的依赖。

```xml
<dependency>
	<groupId>com.robert.kafka</groupId>
	<artifactId>kclient-core</artifactId>
	<version>0.0.1</version>
</dependency>
```

3).根据[Kafka官方文档](http://kafka.apache.org/documentation.html)搭建Kafka环境，并创建两个Topic， test1和test2。

4).然后，从Kafka安装目录的config目录下拷贝kafka-consumer.properties和kafka-producer.properties到你的项目类路径下，通常是src/main/resources目录。

**1.Java API**

Java API提供了最直接，最简单的使用KClient的方法。

*构建Producer示例：*

```java
KafkaProducer kafkaProducer = new KafkaProducer("kafka-producer.properties", "test");

for (int i = 0; i < 10; i++) {
	Dog dog = new Dog();
	dog.setName("Yours " + i);
	dog.setId(i);
	kafkaProducer.sendBean2Topic("test", dog);

	System.out.format("Sending dog: %d \n", i + 1);

	Thread.sleep(100);
}
```

*构建Consumer示例：*

```java
DogHandler mbe = new DogHandler();

KafkaConsumer kafkaConsumer = new KafkaConsumer("kafka-consumer.properties", "test", 1, mbe);
try {
	kafkaConsumer.startup();

	try {
		System.in.read();
	} catch (IOException e) {
		e.printStackTrace();
	}
} finally {
	kafkaConsumer.shutdownGracefully();
}
 ```
 
```java
public class DogHandler extends BeanMessageHandler<Dog> {
	public DogHandler() {
		super(Dog.class);
	}

	protected void doExecuteBean(Dog dog) {
		System.out.format("Receiving dog: %s\n", dog);
	}
}
``` 

**2.Spring环境集成**

KClient可以与Spring环境无缝集成，你可以像使用Spring Bean一样来使用KafkaProducer和KafkaConsumer。

*构建Producer示例：*

```java
ApplicationContext ac = new ClassPathXmlApplicationContext("kafka-producer.xml");

KafkaProducer kafkaProducer = (KafkaProducer) ac.getBean("producer");

for (int i = 0; i < 10; i++) {
	Dog dog = new Dog();
	dog.setName("Yours " + i);
	dog.setId(i);
	kafkaProducer.send2Topic("test", JSON.toJSONString(dog));

	System.out.format("Sending dog: %d \n", i + 1);

	Thread.sleep(100);
}
```

```xml
<bean name="producer" class="com.robert.kafka.kclient.core.KafkaProducer" init-method="init">
	<property name="propertiesFile" value="kafka-producer.properties"/>
	<property name="defaultTopic" value="test"/>
</bean>
```

*构建Consumer示例：*

```java
ApplicationContext ac = new ClassPathXmlApplicationContext(
		"kafka-consumer.xml");

KafkaConsumer kafkaConsumer = (KafkaConsumer) ac.getBean("consumer");
try {
	kafkaConsumer.startup();

	try {
		System.in.read();
	} catch (IOException e) {
		e.printStackTrace();
	}
} finally {
	kafkaConsumer.shutdownGracefully();
}
 ```
 
```java
public class DogHandler extends BeanMessageHandler<Dog> {
	public DogHandler() {
		super(Dog.class);
	}

	protected void doExecuteBean(Dog dog) {
		System.out.format("Receiving dog: %s\n", dog);
	}
}
``` 
 
```xml
<bean name="dogHandler" class="com.robert.kafka.kclient.sample.api.DogHandler" />

<bean name="consumer" class="com.robert.kafka.kclient.core.KafkaConsumer" init-method="init">
	<property name="propertiesFile" value="kafka-consumer.properties" />
	<property name="topic" value="test" />
	<property name="streamNum" value="1" />
	<property name="handler" ref="dogHandler" />
</bean>
```
 
**3.服务源码注解**

KClient提供了类似Spring声明式的编程方法，使用注解声明Kafka处理器方法，所有的线程模型、异常处理、服务启动和关闭等都由后台服务自动完成，极大程度的简化了API的使用方法，提高了开发者的工作效率。

*注解声明Kafka消息处理器：*

```java
@KafkaHandlers
public class AnnotatedDogHandler {
	@InputConsumer(propertiesFile = "kafka-consumer.properties", topic = "test", streamNum = 1)
	@OutputProducer(propertiesFile = "kafka-producer.properties", defaultTopic = "test1")
	public Cat dogHandler(Dog dog) {
		System.out.println("Annotated dogHandler handles: " + dog);

		return new Cat(dog);
	}

	@InputConsumer(propertiesFile = "kafka-consumer.properties", topic = "test1", streamNum = 1)
	public void catHandler(Cat cat) throws IOException {
		System.out.println("Annotated catHandler handles: " + cat);

		throw new IOException("Man made exception.");
	}

	@ErrorHandler(exception = IOException.class, topic = "test1")
	public void ioExceptionHandler(IOException e, String message) {
		System.out.println("Annotated excepHandler handles: " + e);
	}
}
```

*注解启动程序：*

```java
public static void main(String[] args) {
	ApplicationContext ac = new ClassPathXmlApplicationContext(
			"annotated-kafka-consumer.xml");

	try {
		System.in.read();
	} catch (IOException e) {
		e.printStackTrace();
	}
}
```

*注解Spring环境配置：*

```xml
<bean name="kClientBoot" class="com.robert.kafka.kclient.boot.KClientBoot" init-method="init"/>

<context:component-scan base-package="com.robert.kafka.kclient.sample.annotation" />
```

## API简介

**1.Producer API**

KafkaProducer类提供了丰富的API来发送不同类型的消息，它支持发送字符串消息，发送一个普通的Bean，以及发送JSON对象等。在这些API中可以指定发送到某个Topic，也可以不指定而使用默认的Topic。对于发送的数据，支持带Key值的消息和不带Key值的消息。

*发送字符串消息:*

```java
public void send(String message);
public void send2Topic(String topicName, String message); 
public void send(String key, String message); 
public void send2Topic(String topicName, String key, String message); 
public void send(Collection<String> messages); 
public void send2Topic(String topicName, Collection<String> messages); 
public void send(Map<String, String> messages); 
public void send2Topic(String topicName, Map<String, String> messages); 
```

*发送Bean消息:*

```java
public <T> void sendBean(T bean); 
public <T> void sendBean2Topic(String topicName, T bean); 
public <T> void sendBean(String key, T bean); 
public <T> void sendBean2Topic(String topicName, String key, T bean); 
public <T> void sendBeans(Collection<T> beans); 
public <T> void sendBeans2Topic(String topicName, Collection<T> beans); 
public <T> void sendBeans(Map<String, T> beans); 
public <T> void sendBeans2Topic(String topicName, Map<String, T> beans);
```

*发送JSON对象消息:*

```java
public void sendObject(JSONObject jsonObject); 
public void sendObject2Topic(String topicName, JSONObject jsonObject); 
public void sendObject(String key, JSONObject jsonObject); 
public void sendObject2Topic(String topicName, String key, JSONObject jsonObject); 
public void sendObjects(JSONArray jsonArray); 
public void sendObjects2Topic(String topicName, JSONArray jsonArray); 
public void sendObjects(Map<String, JSONObject> jsonObjects); 
public void sendObjects2Topic(String topicName, Map<String, JSONObject> jsonObjects); 
```

**2.Consumer API**

KafkaConsumer类提供了丰富的构造函数用来指定Kafka消费者服务器的各项参数，包括线程池策略，线程池类型，流数量等等。

*使用PROPERTIES文件初始化:*

```java
public KafkaConsumer(String propertiesFile, String topic, int streamNum, MessageHandler handler);
public KafkaConsumer(String propertiesFile, String topic, int streamNum, int fixedThreadNum, MessageHandler handler);
public KafkaConsumer(String propertiesFile, String topic, int streamNum, int fixedThreadNum, boolean isSharedThreadPool, MessageHandler handler);
public KafkaConsumer(String propertiesFile, String topic, int streamNum, int minThreadNum, int maxThreadNum, MessageHandler handler);
public KafkaConsumer(String propertiesFile, String topic, int streamNum, int minThreadNum, int maxThreadNum, boolean isSharedThreadPool,MessageHandler handler);
```

*使用PROPERTIES对象初始化:* 

```java
public KafkaConsumer(Properties properties, String topic, int streamNum, MessageHandler handler);
public KafkaConsumer(Properties properties, String topic, int streamNum, int fixedThreadNum, MessageHandler handler);
public KafkaConsumer(Properties properties, String topic, int streamNum, int fixedThreadNum, boolean isSharedThreadPool, MessageHandler handler);
public KafkaConsumer(Properties properties, String topic, int streamNum, int minThreadNum, int maxThreadNum, MessageHandler handler);
public KafkaConsumer(Properties properties, String topic, int streamNum, int minThreadNum, int maxThreadNum, boolean isSharedThreadPool,MessageHandler handler);
```

**3.消息处理器**

消息处理器结构提供了一个基本接口，并且提供了不同的抽象类实现不同层次的功能，让功能得到最大化的重用，并且互相解偶，开发者可以根据需求选择某一个抽象类来继承和使用。

*接口定义:*

```java
public interface MessageHandler {
	public void execute(String message);
}
```

*安全处理异常抽象类:*

```java
public abstract class SafelyMessageHandler implements MessageHandler {
	public void execute(String message) {
		try {
			doExecute(message);
		} catch (Throwable t) {
			handleException(t, message);
		}
	}
	
	protected void handleException(Throwable t, String message) {
		for (ExceptionHandler excepHandler : excepHandlers) {
			if (t.getClass() == IllegalStateException.class
					&& t.getCause() != null
					&& t.getCause().getClass() == InvocationTargetException.class
					&& t.getCause().getCause() != null)
				t = t.getCause().getCause();
	
			if (excepHandler.support(t)) {
				try {
					excepHandler.handle(t, message);
				} catch (Exception e) {
					log.error(
							"Exception hanppens when the handler {} is handling the exception {} and the message {}. Please check if the exception handler is configured properly.",
							excepHandler.getClass(), t.getClass(), message);
					log.error(
							"The stack of the new exception on exception is, ",
							e);
				}
			}
		}
	}
}

protected abstract void doExecute(String message);
```

*面向类型的抽象类:*

```java

public abstract class BeanMessageHandler<T> extends SafelyMessageHandler {...}
public abstract class BeansMessageHandler<T> extends SafelyMessageHandler {...}
public abstract class DocumentMessageHandler extends SafelyMessageHandler {...}
public abstract class ObjectMessageHandler extends SafelyMessageHandler {...}
public abstract class ObjectsMessageHandler extends SafelyMessageHandler {...}
```

**4.消息处理器注解**

正如上面使用指南第三部分服务源码注解所讲述的那样，KClient可以通过注解来声明Kafka消息处理器，KClient提供了@KafkaHandlers、@InputConsumer、@OutputProducer和@ErrorHandler等注解。

*@KafkaHandlers:*

```java
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface KafkaHandlers {
}
```

*@InputConsumer:*

```java
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface InputConsumer {
	String propertiesFile() default "";

	String topic() default "";

	int streamNum() default 1;

	int fixedThreadNum() default 0;

	int minThreadNum() default 0;

	int maxThreadNum() default 0;
}
```

*@OutputProducer:*

```java
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OutputProducer {
	String propertiesFile() default "";

	String defaultTopic() default "";
}
```

*@ErrorHandler:*

```java
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ErrorHandler {
	Class<? extends Throwable> exception() default Throwable.class;

	String topic() default "";
}
```

## 消息处理机模板项目

### 快速开发向导

通过下面步骤可以快速开发Kafka处理机服务。

1.从本项目下载kclient-processor项目模板，并且根据业务需要修改pom.xml后导入Eclipse。

2.根据业务需要更改`com.robert.kclient.app.handler.AnimalsHandler`类名称，并且根据业务需要修改处理器的注解。这里，可以导入业务服务对消息进行处理。

```java
@KafkaHandlers
public class AnimalsHandler {
	@InputConsumer(propertiesFile = "kafka-consumer.properties", topic = "test", streamNum = 1)
	@OutputProducer(propertiesFile = "kafka-producer.properties", defaultTopic = "test1")
	public Cat dogHandler(Dog dog) {
		System.out.println("Annotated dogHandler handles: " + dog);

		return new Cat(dog);
	}

	@InputConsumer(propertiesFile = "kafka-consumer.properties", topic = "test1", streamNum = 1)
	public void catHandler(Cat cat) throws IOException {
		System.out.println("Annotated catHandler handles: " + cat);

		throw new IOException("Man made exception.");
	}

	@ErrorHandler(exception = IOException.class, topic = "test1")
	public void ioExceptionHandler(IOException e, String message) {
		System.out.println("Annotated excepHandler handles: " + e);
	}
}
``` 
3.通过`mvn package`既可以打包包含Spring Boot功能的自启动jar包。

4.通过`java -jar kclient-processor.jar`即可启动服务。

### 后台监控和管理

KClient模板项目提供了后台管理接口来监控和管理消息处理服务。

1.欢迎词 - 用来校验服务是否启动成功。

>curl http://localhost:8080/

2.服务状态 - 显示处理器数量。

>curl http://localhost:8080/status

3.重启服务 - 重新启动服务。

>curl http://localhost:8080/restart

## 架构设计

**1.线程模型**

*同步线程模型:*

在这种线程模型中，客户端为每一个消费者流使用一个线程，每个线程负责从Kafka队列里消费消息，并且在同一个线程里进行业务处理。我们把这些线程称为消费线程，把这些线程所在的线程池叫做消息消费线程池。这种模型之所以在消息消费线程池处理业务，是因为它多用于处理轻量级别的业务，例如：缓存查询，本地计算等。

*异步线程模型:*

在这种线程模型中，客户端为每一个消费者流使用一个线程，每个线程负责从Kafka队列里消费消息，并且传递消费得到的消息到后端的异步线程池，在异步线程池中处理业务。我们仍然把前面负责消费消息的线程池成为消息消费线程池，把后面的异步线程池成为异步业务线程池。这种线程模型适合重量级的业务，例如：业务中有大量的IO操作，网络IO操作，复杂计算，对外部系统的调用等。

后端的异步业务线程池又细分为所有消费者流共享线程池和每个流独享线程池。

所有消费者流共享线程池对比每个流独享线程池，创建更少的线程池对象，能节省些许的内存，但是，由于多个流共享同一个线程池，在数据量较大的时候，流之间的处理可能互相影响。例如，一个业务使用2个区和两个流，他们一一对应，通过生产者指定定制化的散列函数替换默认的key-hash, 实现一个流（区）用来处理普通用户，另外一个流（区）用来处理VIP用户，如果两个流共享一个线程池，当普通用户的消息大量产生的时候，VIP用户只有少量，并且排在了队列的后头，就会产生饿死的情况。这个场景是可以使用多个topic来解决，一个普通用户的topic，一个VIP用户的topic，但是这样又要多维护一个topic，客户端发送的时候需要显式的进行判断topic目标，也没有多少好处。

每个流独享线程池使用不同的异步业务线程池来处理不同的流里面的消息，互相隔离，互相独立，不互相影响，对于不同的流（区）的优先级不同的情况，或者消息在不同流（区）不均衡的情况下表现会更好，当然，创建多个线程池会多使用些许内存，但是这并不是一个大问题。

另外，异步业务线程池支持确定数量线程的线程池和线程数量可伸缩的线程池。核心业务硬件资源有保证，核心服务有专享的资源池，或者线上流量可预测，请使用固定数量的线程池。非核心业务一般混布，资源互相调配，线上流量不固定等情况请使用线程数量可伸缩的线程池。

**2.异常处理**

*消息处理产生的业务异常:*

当前在业务处理的上层捕捉了Throwable, 在专用的错误恢复日志中记录出错的消息，后续可根据错误恢复日志人工处理错误消息，也可重做或者洗数据。`TODO：`考虑实现异常Listener体系结构, 对异常处理实现监听者模式，异常处理器可插拔等，默认打印错误日志。

由于默认的异常处理中，捕捉异常，在专用的错误回复日志中记录错误，并且继续处理下一个消息。考虑到可能上线失败，或者上游消息格式出错，导致所有消息处理都出错，堆满错误恢复日志的情况，我们需要诉诸于报警和监控系统来解决。 

**3.优雅关机**

由于消费者本身是一个事件驱动的服务器，类似Tomcat，Tomcat接收HTTP请求返回HTTP响应，Consumer则接收Kafka消息，然后处理业务后返回，也可以将处理结果发送到下一个消息队列。所以消费者本身是非常复杂的，除了线程模型，异常处理，性能，稳定性，可用性等都是需要思考点。既然消费者是一个后台的服务器，我们需要考虑如何优雅的关机，也就是在消费者服务器在处理消息的时候，如果关机才能不导致处理的消息中断而丢失。

优雅关机的重点在于：1. 如何知道JVM要退出; 2. 如何阻止Daemon的线程在JVM退出被杀掉而导致消息丢失; 3. 如果Worker线程在阻塞，如何唤起并退出。  

*第一个问题:*，如果一个后台程序运行在控制台的前台，通过`Ctrl + C`可以发送退出信号给JVM，也可以通过`kill -2 PS_IS` 或者 `kill -15 PS_IS`发送退出信号，但是不能发送`kill -9 PS_IS`, 否则进程会无条件强制退出。JVM收到退出信号后，会调用注册的钩子，我们通过的注册的JVM退出钩子进行优雅关机。

*第二个问题:*，线程分为Daemon线程和非Daemon线程，一个线程默认继承父线程的Daemon属性，如果当前线程池是由Daemon线程创建的，则Worker线程是Daemon线程。如果Worker线程是Daemon线程，我们需要在JVM退出钩子中等待Worker线程完成当前手头处理的消息，再退出JVM。如果不是Daemon线程，即使JVM收到退出信号，也得等待Worker线程退出后再退出，不会丢掉正在处理的消息。

*第三个问题:*，在Worker线程从Kafka服务器消费消息的时候，Worker线程可能处于阻塞，这时需要中断线程以退出，没有消息被丢掉。在Worker线程处理业务时有可能有阻塞，例如：IO，网络IO，在指定退出时间内没有完成，我们也需要中断线程退出，这时会产生一个InterruptedException, 在异常处理的默认处理器中被捕捉，并写入错误日志，Worker线程随后退出。

## 性能压测

`TODO` 

Benchmark应该覆盖推送QPS，接收处理QPS以及单线程和多线程生产者的用例。

*用例1：* 轻量级服务同步线程模型和异步线程模型的性能对比。
*用例2：* 重量级服务同步线程模型和异步线程模型的性能对比。
*用例3：* 重量级服务异步线程模型中所有消费者流共享线程池和每个流独享线程池的性能对比。
*用例4：* 重量级服务异步线程模型中每个流独享线程池的对比的确定数量线程的线程池和线程数量可伸缩的线程池的性能对比。

## TODO

1. 性能压测还没有完成。
2. KClient处理器项目中管理Rest服务的丰富，增加对线程池的监控，以及消息处理性能的监控。
3. 让@ErrorHandler里面的exception成为可选，使用方法第一参数进行推测。
4. 给模板项目增加启动和关闭脚本。
5. 实现消息发送端的可靠投递，在发送超时的时候，持久到数据库或者缓存，定时补偿发送，也就是实现Producer可以持久消息到数据库，有问题的时候，异步定时重发
6. InputConsumer和OutputProducer中的propertiesFile有些多余，我们需要把它迁移到KafkaHandlers中，或者支持多个地方配置，或者支持具体每个属性在方法中的配置。
7. InterruptedException和优雅关机的实现，优化关机的实现还不健全，it.hasNext()退出后，最后一个chunk没有被提交, Kafka0.8版本提交默认是30秒一次的,读出来没提交很正常

## 获得技术支持

联系作者：robert_lyp

<a href="./resource/images/myqrcode.jpeg"><img src="./resource/images/myqrcode.jpeg" alt="cloudate-qrcode" width="150" height="190" class="alignnone size-full wp-image-1138" /></a>

