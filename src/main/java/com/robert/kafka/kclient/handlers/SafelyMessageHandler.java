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

	// Write down the error log for data recovery

	protected static Logger errorRunLog = LoggerFactory.getLogger("error.run."
			+ SafelyMessageHandler.class);
	protected static Logger shutdownErrorlog = LoggerFactory
			.getLogger("error.shutdown." + SafelyMessageHandler.class);

	public void execute(String message) {
		try {
			doExecute(message);
		} catch (Throwable t) {
			if (t instanceof InterruptedException)
				errorRunLog.error(
						"Maybe shutting down. Interruped when handing the message:\t"
								+ message, t);
			else
				shutdownErrorlog.error("Failed to handle the message:\t"
						+ message, t);
		}
	}

	protected abstract void doExecute(String message);
}
