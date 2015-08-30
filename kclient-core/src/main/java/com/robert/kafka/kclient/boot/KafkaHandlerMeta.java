package com.robert.kafka.kclient.boot;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to store the handler related information by reflection. These
 * information will be used to call the handler by reflection subsequently.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaHandlerMeta {
	private Object bean;

	private Method method;

	private Class<? extends Object> parameterType;

	private InputConsumer inputConsumer;

	private OutputProducer outputProducer;

	private Map<ErrorHandler, Method> errorHandlers = new HashMap<ErrorHandler, Method>();

	public Object getBean() {
		return bean;
	}

	public void setBean(Object bean) {
		this.bean = bean;
	}

	public Method getMethod() {
		return method;
	}

	public void setMethod(Method method) {
		this.method = method;
	}

	public Class<? extends Object> getParameterType() {
		return parameterType;
	}

	public void setParameterType(Class<? extends Object> parameterType) {
		this.parameterType = parameterType;
	}

	public InputConsumer getInputConsumer() {
		return inputConsumer;
	}

	public void setInputConsumer(InputConsumer inputConsumer) {
		this.inputConsumer = inputConsumer;
	}

	public OutputProducer getOutputProducer() {
		return outputProducer;
	}

	public void setOutputProducer(OutputProducer outputProducer) {
		this.outputProducer = outputProducer;
	}

	public Map<ErrorHandler, Method> getErrorHandlers() {
		return errorHandlers;
	}

	public void setErrorHandlers(Map<ErrorHandler, Method> errorHandlers) {
		this.errorHandlers = errorHandlers;
	}

	public void addErrorHandlers(Map<ErrorHandler, Method> errorHandlers) {
		this.errorHandlers.putAll(errorHandlers);
	}

	public void addErrorHandlers(ErrorHandler errorHandler, Method method) {
		this.errorHandlers.put(errorHandler, method);
	}

}
