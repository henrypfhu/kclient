package com.robert.kclient.app;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * Spring boot main class which starts spring rest controller in the same
 * package.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
@SpringBootApplication
public class KClientApplication {
	protected static Logger log = LoggerFactory
			.getLogger(KClientApplication.class);

	public static void main(String[] args) {
		ApplicationContext ctxBackend = SpringApplication.run(
				KClientApplication.class, args);

		String startupTime = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z")
				.format(new Date(ctxBackend.getStartupDate()));
		log.info("KClient application starts at: " + startupTime);

		System.out.println("KClient application starts at: " + startupTime);
	}
}
