package com.robert.kclient.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class KClientApplication {
	public static void main(String[] args) {
		ApplicationContext ctxBackend = SpringApplication.run(
				KClientApplication.class, args);

		System.out.println("KClient application is started.");
	}
}
