package com.robert.kclient.app;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.robert.kafka.kclient.boot.KClientBoot;

@RestController
public class KClientController {

	private ApplicationContext ctxKafkaProcessor = new ClassPathXmlApplicationContext(
			"kafka-application.xml");

	@RequestMapping("/")
	public String hello() {
		return "Greetings from kclient processor!";
	}

	@RequestMapping("/status")
	public String status() {
		return "Producer: " + getKClientBoot().getKafkaProducers().size()
				+ ", Consumer: " + getKClientBoot().getKafkaConsumers().size();
	}

	private KClientBoot getKClientBoot() {
		return (KClientBoot) ctxKafkaProcessor.getBean("kClientBoot");
	}

	@RequestMapping("/restart")
	public String restart() {
		getKClientBoot().shutdownAll();
		ctxKafkaProcessor = new ClassPathXmlApplicationContext(
				"kafka-application.xml");

		return "KClient processor is restarted.";
	}
}