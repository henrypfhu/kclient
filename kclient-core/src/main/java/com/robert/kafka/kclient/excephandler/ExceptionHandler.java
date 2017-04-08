package com.robert.kafka.kclient.excephandler;

/**
 * This is the pluggable interface to handle the unexpected exception when
 * handing the message in handlers.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
public interface ExceptionHandler {
	public boolean support(Throwable t);

	public void handle(Throwable t, String message);
}
