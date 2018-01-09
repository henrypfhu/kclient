package com.robert.kclient.app;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.robert.kafka.kclient.boot.KClientBoot;

/**
 * Backend controller used to monitor and restart the KClient.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

@RestController
public class KClientController {
	protected static Logger log = LoggerFactory
			.getLogger(KClientApplication.class);

	private ApplicationContext ctxKafkaProcessor = new ClassPathXmlApplicationContext(
			"kafka-application.xml");

	@RequestMapping("/")
	public String hello() {
		return "Greetings from kclient processor!";
	}

	@RequestMapping("/status")
	public String status() {
		return "Handler Number: [" + getKClientBoot().getKafkaHandlers().size()
				+ "]";
	}

	@RequestMapping("/stop")
	public String stop() {
		log.info("Shutdowning KClient now...");
		getKClientBoot().shutdownAll();

		String startupTime = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z")
				.format(new Date(ctxKafkaProcessor.getStartupDate()));
		log.info("KClient application stops at: " + startupTime);

		return "KClient application stops at: " + startupTime;
	}

	@RequestMapping("/restart")
	public String restart() {
		log.info("Shutdowning KClient now...");
		getKClientBoot().shutdownAll();

		log.info("Restarting KClient now...");
		ctxKafkaProcessor = new ClassPathXmlApplicationContext(
				"kafka-application.xml");

		String startupTime = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z")
				.format(new Date(ctxKafkaProcessor.getStartupDate()));
		log.info("KClient application restarts at: " + startupTime);

		return "KClient application restarts at: " + startupTime;
	}

	private KClientBoot getKClientBoot() {
		return (KClientBoot) ctxKafkaProcessor.getBean("kClientBoot");
	}
}