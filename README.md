# Kafka客户端(KClient)

简单易用，有效集成，高性能，高稳定的Kafka Java客户端。

## 背景

消息队列在互联网领域里得到了广泛的应用，它多应用在异步处理，模块之间的解偶和高并发的消峰等场景，消息队列中表现最好的当属Apache开源项目Kafka，Kafka是使用Scala语言开发，利用操作系统的缓存原理达到高性能，而且有不同语言的客户端，使用起来非常的方便。KClient提供了一个简单易用，有效集成，高性能，高稳定的Kafka Java客户端。

在继续介绍KClient的功能特性，架构设计和使用方法之前,读者需要对Kafka进行基本的学习和了解。如果你是Kafka的初学者或者从未接触过Kafka，请参考我的博客文章：[Kafka的那些事儿](http://cloudate.net/?p=1763)。如果你英文还不错，也可以直接参考Kafka官方在线文档：[Kafka 0.8.2 Documentation](kafka.apache.org/documentation.html).

## 功能特性

**1.简单易用**

简化了Kafka客户端API的使用方法, 特别是对消费端开发，开发者只需要实现MessageHandler接口或者相关子类，在实现中处理消息完成业务逻辑，并且在主线程中启动封装的消费端服务器即可, 提供了各种常用的MessageHandler，框架自动转换消息到领域对象模型或者JSON对象等数据结构， 让开发者更专注于业务处理。尤其是服务源码注解，可以通过注解的方法将一个通用的服务方法声明成消息处理机的后台服务，使用起来非常简单，代码看起来一目了然，性能在框架级别也能有所保障。

它提供了3种使用方法：1. Java API 2.Spring环境集成 3. 服务源码注解， 并且提供了消息处理机的模板项目
 
**2.高性能**

它提供三种线程模型，用以在不同的业务场景下实现高性能：1. 适合轻量级服务的同步模型 2. 适合IO密集型服务的异步线程模型（所有流共享线程池和每个流独享线程池）， 另外异步模型中的线程池也支持确定线程池和可伸缩的线程池。

**3.高稳定性**：

框架级别处理了通用的异常，计入错误日志，可用于错误手工恢复或者洗数据，并实现了优雅关机和重启等功能

## 使用指南

KClient提供了三种使用方法，

**1.Java API** 

**2.Spring环境集成**
 
**3.服务源码注解**

## 消息处理机模板项目

## 架构设计

**1.线程模型**

消息消费线程中处理业务和异步线程池处理业务

同步线程模型：适合短小操作
异步线程模型：适合IO操作

共享线程池的问题是，所有流使用同一个线程池处理，如果partition有优先级，可能会发生饿死，这个可以使用多个queue来解决，但是多个queue需要维护多个queue，而且在客户端判断，并没有减轻工作量


**2.异常处理**

1. 如果某个线程中处理某条消息失败怎么办？如果大量失败怎么办？
2. 并发的处理器中，如果断电了，怎么回复
3. Interrupted exception and normal exception
4. 出错将当前消息消费计入log，如果过多异常，期待监控和报警系统发现
5. 洗数据和手工重做

**3.优雅关机**

3. 研究关机的时候哪些信号会杀死线程，deamon和非deamon线程的区别

**4.API设计**

consumer api
producer api
Handler类的集成结构
注解
boot

## 性能压测

同步线程模型
异步线程模型，共享线程池，非共享线程池，固定线程池和可变线程池

## TODO

1. 做一个j2ee项目模板，使用Listener启动consumer
2. 做一个使用Annotation启动的consumer, @Consumer(propertiesFile="", topic="", streamNum="", fixedThreadNum=""), 使用一个启动bean
	@Producer(propertiesFile="", topic=""), 使用spring boot启动

## QQ群/微信公众号
- <a target="_blank" href="http://shang.qq.com/wpa/qunwpa?idkey=ff0d7d34f32c87dbd9aa56499a7478cd93e0e1d44288b9f6987a043818a1ad01"><img border="0" src="http://pub.idqqimg.com/wpa/images/group.png" alt="云时代网" title="云时代网"></a>
<br>
- <a href="http://cloudate.net/wp-content/uploads/2015/01/cloudate-qrcode.jpg"><img src="http://cloudate.net/wp-content/uploads/2015/01/cloudate-qrcode.jpg" alt="cloudate-qrcode" width="90" height="90" class="alignnone size-full wp-image-1138" /></a>

## 关于作者
- 罗伯特出品   微信： 13436881186