package com.robert.kafka.kclient.handlers;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.robert.kafka.kclient.excephandler.ExceptionHandler;

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

	public ObjectsMessageHandler() {
		super();
	}

	public ObjectsMessageHandler(ExceptionHandler excepHandler) {
		super(excepHandler);
	}

	public ObjectsMessageHandler(List<ExceptionHandler> excepHandlers) {
		super(excepHandlers);
	}

	protected void doExecute(String message) {
		JSONArray jsonArray = JSON.parseArray(message);
		doExecuteObjects(jsonArray);
	}

	protected abstract void doExecuteObjects(JSONArray jsonArray);
}
