package com.robert.kafka.kclient.boot;

import java.lang.reflect.Method;

public class KafkaHandlerMeta {
	private Object bean;

	private Method method;

	private Class<? extends Object> parameterType;

	private InputConsumer inputConsumer;

	private OutputProducer outputProducer;

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
}
