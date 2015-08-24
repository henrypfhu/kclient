package com.robert.kafka.kclient.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an abstract class which log exception in log file if it happens.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
public abstract class SafelyMessageHandler implements MessageHandler {
	protected static Logger log = LoggerFactory
			.getLogger(SafelyMessageHandler.class);

	public void execute(String message) {
		try {
			doExecute(message);
		} catch (Throwable t) {
			log.error("Failed to handle the message:\t" + message, t);
		}
	}

	protected abstract void doExecute(String message);
}
