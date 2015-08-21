package com.robert.kafka.kclient;

/**
 * This is the exposed interface which the business component should implement
 * to handle the Kafka message.
 * 
 * @author Robert
 * @since Aug 21, 2015
 *
 */
public interface MessageExecutor {
	public void execute(String message);
}
