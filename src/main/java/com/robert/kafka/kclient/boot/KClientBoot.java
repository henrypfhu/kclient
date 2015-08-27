package com.robert.kafka.kclient.boot;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.robert.kafka.kclient.core.KafkaConsumer;
import com.robert.kafka.kclient.core.KafkaProducer;
import com.robert.kafka.kclient.handlers.BeanMessageHandler;
import com.robert.kafka.kclient.handlers.BeansMessageHandler;
import com.robert.kafka.kclient.handlers.MessageHandler;
import com.robert.kafka.kclient.handlers.ObjectMessageHandler;
import com.robert.kafka.kclient.handlers.ObjectsMessageHandler;

public class KClientBoot implements ApplicationContextAware {
	protected static Logger log = LoggerFactory.getLogger(KClientBoot.class);

	private ApplicationContext applicationContext;

	final List<KafkaConsumer> kafkaConsumers = new ArrayList<KafkaConsumer>();

	final List<KafkaProducer> kafkaProducers = new ArrayList<KafkaProducer>();

	public KClientBoot() {
		// For Spring Context
	}

	public void init() {
		List<KafkaHandlerMeta> meta = getKafkaHandlerMeta();

		if (meta.size() == 0)
			throw new IllegalArgumentException(
					"No handler method is declared in this spring context.");

		for (final KafkaHandlerMeta kafkaHandlerMeta : meta) {
			createKafkaHandler(kafkaHandlerMeta);
		}
	}

	protected List<KafkaHandlerMeta> getKafkaHandlerMeta() {
		List<KafkaHandlerMeta> meta = new ArrayList<KafkaHandlerMeta>();

		String[] kafkaHandlerBeanNames = applicationContext
				.getBeanNamesForAnnotation(KafkaHandlers.class);

		for (String kafkaHandlerBeanName : kafkaHandlerBeanNames) {
			Object kafkaHandlerBean = applicationContext
					.getBean(kafkaHandlerBeanName);

			Class<? extends Object> kafkaHandlerBeanClazz = kafkaHandlerBean
					.getClass();

			for (Method kafkaHandlerMethod : kafkaHandlerBeanClazz.getMethods()) {
				InputConsumer inputConsumer = null;
				OutputProducer outputProduer = null;
				for (Annotation kafkaHandlerAnnotation : kafkaHandlerMethod
						.getAnnotations()) {
					if (kafkaHandlerAnnotation instanceof InputConsumer) {
						if (inputConsumer != null)
							throw new IllegalArgumentException(
									"Duplicated InputConsumer in a single handler method.");

						inputConsumer = (InputConsumer) kafkaHandlerAnnotation;
					} else if (kafkaHandlerAnnotation instanceof OutputProducer) {
						if (outputProduer != null)
							throw new IllegalArgumentException(
									"Duplicated InputConsumer in a single handler method.");

						outputProduer = (OutputProducer) kafkaHandlerAnnotation;
					}
				}

				if (inputConsumer != null) {
					KafkaHandlerMeta kafkaHandlerMeta = new KafkaHandlerMeta();
					kafkaHandlerMeta.setInputConsumer(inputConsumer);
					kafkaHandlerMeta.setOutputProducer(outputProduer);

					kafkaHandlerMeta.setBean(kafkaHandlerBean);
					kafkaHandlerMeta.setMethod(kafkaHandlerMethod);

					Parameter[] kafkaHandlerParameters = kafkaHandlerMethod
							.getParameters();
					if (kafkaHandlerParameters.length != 1)
						throw new IllegalArgumentException(
								"The kafka handler method can contains only one parameter.");
					kafkaHandlerMeta.setParameterType(kafkaHandlerParameters[0]
							.getType());

					meta.add(kafkaHandlerMeta);
				}
			}
		}

		return meta;
	}

	protected void createKafkaHandler(final KafkaHandlerMeta kafkaHandlerMeta) {
		Class<? extends Object> paramClazz = kafkaHandlerMeta
				.getParameterType();

		KafkaProducer kafkaProducer = createProducer(kafkaHandlerMeta);

		MessageHandler beanMessageHandler = null;
		if (paramClazz.isAssignableFrom(JSONObject.class)) {
			beanMessageHandler = createObjectHandler(kafkaHandlerMeta,
					kafkaProducer);

		} else if (paramClazz.isAssignableFrom(JSONArray.class)) {
			beanMessageHandler = createObjectsHandler(kafkaHandlerMeta,
					kafkaProducer);
		} else if (List.class.isAssignableFrom(paramClazz)) {
			beanMessageHandler = createBeansHandler(kafkaHandlerMeta,
					kafkaProducer);

		} else {
			beanMessageHandler = createBeanHandler(kafkaHandlerMeta,
					kafkaProducer);

		}

		KafkaConsumer kafkaConsumer = createConsumer(kafkaHandlerMeta,
				beanMessageHandler);
		kafkaConsumer.startup();

	}

	protected ObjectMessageHandler<JSONObject> createObjectHandler(
			final KafkaHandlerMeta kafkaHandlerMeta,
			final KafkaProducer kafkaProducer) {

		ObjectMessageHandler<JSONObject> objectMessageHandler = new ObjectMessageHandler<JSONObject>() {
			@Override
			protected void doExecuteObject(JSONObject jsonObject) {
				invokeHandler(kafkaHandlerMeta, kafkaProducer, jsonObject);
			}

		};

		return objectMessageHandler;
	}

	protected ObjectsMessageHandler<JSONArray> createObjectsHandler(
			final KafkaHandlerMeta kafkaHandlerMeta,
			final KafkaProducer kafkaProducer) {

		ObjectsMessageHandler<JSONArray> objectMessageHandler = new ObjectsMessageHandler<JSONArray>() {
			@Override
			protected void doExecuteObjects(JSONArray jsonArray) {
				invokeHandler(kafkaHandlerMeta, kafkaProducer, jsonArray);
			}
		};

		return objectMessageHandler;
	}

	@SuppressWarnings("unchecked")
	protected BeanMessageHandler<Object> createBeanHandler(
			final KafkaHandlerMeta kafkaHandlerMeta,
			final KafkaProducer kafkaProducer) {

		// We have to abandon the type check
		@SuppressWarnings("rawtypes")
		BeanMessageHandler beanMessageHandler = new BeanMessageHandler(
				kafkaHandlerMeta.getParameterType()) {
			@Override
			protected void doExecuteBean(Object bean) {
				invokeHandler(kafkaHandlerMeta, kafkaProducer, bean);
			}

		};

		return beanMessageHandler;
	}

	@SuppressWarnings("unchecked")
	protected BeansMessageHandler<Object> createBeansHandler(
			final KafkaHandlerMeta kafkaHandlerMeta,
			final KafkaProducer kafkaProducer) {

		// We have to abandon the type check
		@SuppressWarnings("rawtypes")
		BeansMessageHandler beanMessageHandler = new BeansMessageHandler(
				kafkaHandlerMeta.getParameterType()) {
			@Override
			protected void doExecuteBeans(List bean) {
				invokeHandler(kafkaHandlerMeta, kafkaProducer, bean);
			}

		};

		return beanMessageHandler;
	}

	protected KafkaProducer createProducer(
			final KafkaHandlerMeta kafkaHandlerMeta) {
		KafkaProducer kafkaProducer = null;

		if (kafkaHandlerMeta.getOutputProducer() != null) {
			kafkaProducer = new KafkaProducer(kafkaHandlerMeta
					.getOutputProducer().propertiesFile(), kafkaHandlerMeta
					.getOutputProducer().defaultTopic());

			kafkaProducers.add(kafkaProducer);
		}

		// It may return null
		return kafkaProducer;
	}

	private void invokeHandler(final KafkaHandlerMeta kafkaHandlerMeta,
			final KafkaProducer kafkaProducer, Object parameter) {
		Method kafkaHandlerMethod = kafkaHandlerMeta.getMethod();
		try {
			Object result = kafkaHandlerMethod.invoke(
					kafkaHandlerMeta.getBean(), parameter);

			if (kafkaProducer != null) {
				kafkaProducer.send(result.toString());
			}
		} catch (IllegalAccessException e) {
			// If annotated config is correct, this won't happen
			log.debug("No permission to access the annotated kafka handler.", e);
			throw new IllegalStateException(
					"No permission to access the annotated kafka handler. Please check annotated config.",
					e);
		} catch (IllegalArgumentException e) {
			// If annotated config is correct, this won't happen
			log.debug(
					"The parameter passed in doesn't match the annotated kafka handler's.",
					e);
			throw new IllegalStateException(
					"The parameter passed in doesn't match the annotated kafka handler's. Please check annotated config.",
					e);
		} catch (InvocationTargetException e) {
			// The SafeMessageHanlder has already handled the
			// throwable, no more exception goes here
			log.debug("Failed to call the annotated kafka handler.", e);
			throw new IllegalStateException(
					"Failed to call the annotated kafka handler. Please check if the handler can handle the biz without any exception.",
					e);
		}
	}

	protected KafkaConsumer createConsumer(
			final KafkaHandlerMeta kafkaHandlerMeta,
			MessageHandler beanMessageHandler) {
		KafkaConsumer kafkaConsumer = null;

		if (kafkaHandlerMeta.getInputConsumer().fixedThreadNum() > 0) {
			kafkaConsumer = new KafkaConsumer(kafkaHandlerMeta
					.getInputConsumer().propertiesFile(), kafkaHandlerMeta
					.getInputConsumer().topic(), kafkaHandlerMeta
					.getInputConsumer().streamNum(), kafkaHandlerMeta
					.getInputConsumer().fixedThreadNum(), beanMessageHandler);

		} else if (kafkaHandlerMeta.getInputConsumer().minThreadNum() == 0
				|| kafkaHandlerMeta.getInputConsumer().maxThreadNum() == 0) {
			kafkaConsumer = new KafkaConsumer(kafkaHandlerMeta
					.getInputConsumer().propertiesFile(), kafkaHandlerMeta
					.getInputConsumer().topic(), kafkaHandlerMeta
					.getInputConsumer().streamNum(), kafkaHandlerMeta
					.getInputConsumer().minThreadNum(), kafkaHandlerMeta
					.getInputConsumer().maxThreadNum(), beanMessageHandler);

		} else {
			kafkaConsumer = new KafkaConsumer(kafkaHandlerMeta
					.getInputConsumer().propertiesFile(), kafkaHandlerMeta
					.getInputConsumer().topic(), kafkaHandlerMeta
					.getInputConsumer().streamNum(), beanMessageHandler);
		}

		kafkaConsumers.add(kafkaConsumer);

		return kafkaConsumer;
	}

	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = applicationContext;
	}

	// The consumers can't be shutdown automatically by jvm shutdown hook if
	// this method is not called
	public void shutdownAll() {
		for (KafkaConsumer kafkaConsumer : kafkaConsumers) {
			kafkaConsumer.shutdownGracefully();
		}

		for (KafkaProducer kafkaProduer : kafkaProducers) {
			kafkaProduer.close();
		}
	}
}
