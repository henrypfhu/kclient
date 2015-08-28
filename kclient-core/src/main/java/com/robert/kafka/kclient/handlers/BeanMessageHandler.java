package com.robert.kafka.kclient.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 * This class converts the JSON string to a single bean, and then make it
 * available to be processed by any subclass.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
public abstract class BeanMessageHandler<T> extends SafelyMessageHandler {
	protected static Logger log = LoggerFactory
			.getLogger(BeanMessageHandler.class);

	private Class<T> clazz;

	public BeanMessageHandler(Class<T> clazz) {
		this.clazz = clazz;
	}

	protected void doExecute(String message) {
		T bean = JSON.parseObject(message, clazz);
		doExecuteBean(bean);
	}

	protected abstract void doExecuteBean(T bean);
}
