package com.robert.kafka.kclient.handlers;

/**
 * This is the exposed interface which the business component should implement
 * to handle the Kafka message.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
public interface MessageHandler {
	public void execute(String message);
}
