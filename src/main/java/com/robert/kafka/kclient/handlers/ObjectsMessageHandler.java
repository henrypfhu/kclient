package com.robert.kafka.kclient.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

/**
 * This class converts the JSON string to a single JSON object, and then make
 * them available to be processed by any subclass.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
public abstract class ObjectsMessageHandler<T> extends SafelyMessageHandler {
	protected static Logger log = LoggerFactory
			.getLogger(ObjectsMessageHandler.class);

	protected void doExecute(String message) {
		JSONArray jsonArray = JSON.parseArray(message);
		doExecuteBean(jsonArray);
	}

	protected abstract void doExecuteBean(JSONArray jsonArray);
}
