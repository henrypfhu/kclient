package com.robert.kafka.kclient.boot;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.StringUtils;
import org.w3c.dom.Document;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.robert.kafka.kclient.core.KafkaConsumer;
import com.robert.kafka.kclient.core.KafkaProducer;
import com.robert.kafka.kclient.excephandler.ExceptionHandler;
import com.robert.kafka.kclient.handlers.BeanMessageHandler;
import com.robert.kafka.kclient.handlers.BeansMessageHandler;
import com.robert.kafka.kclient.handlers.DocumentMessageHandler;
import com.robert.kafka.kclient.handlers.MessageHandler;
import com.robert.kafka.kclient.handlers.ObjectMessageHandler;
import com.robert.kafka.kclient.handlers.ObjectsMessageHandler;
import com.robert.kafka.kclient.reflection.util.AnnotationHandler;
import com.robert.kafka.kclient.reflection.util.AnnotationTranversor;
import com.robert.kafka.kclient.reflection.util.TranversorContext;

/**
 * This is the starter of the annotated message handlers. This class is loaded
 * by any spring context. When the context is post constructed, it will read the
 * annotated message handler, and then create the related consumer or producer.
 * Finally, we start the consumer server.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KClientBoot implements ApplicationContextAware {
	protected static Logger log = LoggerFactory.getLogger(KClientBoot.class);

	private ApplicationContext applicationContext;

	private List<KafkaHandlerMeta> meta = new ArrayList<KafkaHandlerMeta>();

	private List<KafkaHandler> kafkaHandlers = new ArrayList<KafkaHandler>();

	public KClientBoot() {
		// For Spring Context
	}

	public void init() {
		meta = getKafkaHandlerMeta();

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
			Map<Class<? extends Annotation>, Map<Method, Annotation>> mapData = extractAnnotationMaps(kafkaHandlerBeanClazz);

			meta.addAll(convertAnnotationMaps2Meta(mapData, kafkaHandlerBean));
		}

		return meta;
	}

	protected Map<Class<? extends Annotation>, Map<Method, Annotation>> extractAnnotationMaps(
			Class<? extends Object> clazz) {
		AnnotationTranversor<Class<? extends Annotation>, Method, Annotation> annotationTranversor = new AnnotationTranversor<Class<? extends Annotation>, Method, Annotation>(
				clazz);

		Map<Class<? extends Annotation>, Map<Method, Annotation>> data = annotationTranversor
				.tranverseAnnotation(new AnnotationHandler<Class<? extends Annotation>, Method, Annotation>() {

					public void handleMethodAnnotation(
							Class<? extends Object> clazz,
							Method method,
							Annotation annotation,
							TranversorContext<Class<? extends Annotation>, Method, Annotation> context) {
						if (annotation instanceof InputConsumer)
							context.addEntry(InputConsumer.class, method,
									annotation);
						else if (annotation instanceof OutputProducer)
							context.addEntry(OutputProducer.class, method,
									annotation);
						else if (annotation instanceof ErrorHandler)
							context.addEntry(ErrorHandler.class, method,
									annotation);
					}

					public void handleClassAnnotation(
							Class<? extends Object> clazz,
							Annotation annotation,
							TranversorContext<Class<? extends Annotation>, Method, Annotation> context) {
						if (annotation instanceof KafkaHandlers)
							log.warn(
									"There is some other annotation {} rather than @KafkaHandlers in the handler class {}.",
									annotation.getClass().getName(),
									clazz.getName());
					}
				});

		return data;
	}

	protected List<KafkaHandlerMeta> convertAnnotationMaps2Meta(
			Map<Class<? extends Annotation>, Map<Method, Annotation>> mapData,
			Object bean) {
		List<KafkaHandlerMeta> meta = new ArrayList<KafkaHandlerMeta>();

		Map<Method, Annotation> inputConsumerMap = mapData
				.get(InputConsumer.class);
		Map<Method, Annotation> outputProducerMap = mapData
				.get(OutputProducer.class);
		Map<Method, Annotation> exceptionHandlerMap = mapData
				.get(ErrorHandler.class);

		for (Map.Entry<Method, Annotation> entry : inputConsumerMap.entrySet()) {
			InputConsumer inputConsumer = (InputConsumer) entry.getValue();

			KafkaHandlerMeta kafkaHandlerMeta = new KafkaHandlerMeta();

			kafkaHandlerMeta.setBean(bean);
			kafkaHandlerMeta.setMethod(entry.getKey());

			Parameter[] kafkaHandlerParameters = entry.getKey().getParameters();
			if (kafkaHandlerParameters.length != 1)
				throw new IllegalArgumentException(
						"The kafka handler method can contains only one parameter.");
			kafkaHandlerMeta.setParameterType(kafkaHandlerParameters[0]
					.getType());

			kafkaHandlerMeta.setInputConsumer(inputConsumer);

			if (outputProducerMap != null
					&& outputProducerMap.containsKey(entry.getKey()))
				kafkaHandlerMeta
						.setOutputProducer((OutputProducer) outputProducerMap
								.get(entry.getKey()));

			if (exceptionHandlerMap != null)
				for (Map.Entry<Method, Annotation> excepHandlerEntry : exceptionHandlerMap
						.entrySet()) {
					ErrorHandler eh = (ErrorHandler) excepHandlerEntry
							.getValue();
					if (StringUtils.isEmpty(eh.topic())
							|| eh.topic().equals(inputConsumer.topic())) {
						kafkaHandlerMeta.addErrorHandlers((ErrorHandler) eh,
								excepHandlerEntry.getKey());
					}
				}

			meta.add(kafkaHandlerMeta);
		}

		return meta;
	}

	protected void createKafkaHandler(final KafkaHandlerMeta kafkaHandlerMeta) {
		Class<? extends Object> paramClazz = kafkaHandlerMeta
				.getParameterType();

		KafkaProducer kafkaProducer = createProducer(kafkaHandlerMeta);
		List<ExceptionHandler> excepHandlers = createExceptionHandlers(kafkaHandlerMeta);

		MessageHandler beanMessageHandler = null;
		if (paramClazz.isAssignableFrom(JSONObject.class)) {
			beanMessageHandler = createObjectHandler(kafkaHandlerMeta,
					kafkaProducer, excepHandlers);
		} else if (paramClazz.isAssignableFrom(JSONArray.class)) {
			beanMessageHandler = createObjectsHandler(kafkaHandlerMeta,
					kafkaProducer, excepHandlers);
		} else if (List.class.isAssignableFrom(Document.class)) {
			beanMessageHandler = createDocumentHandler(kafkaHandlerMeta,
					kafkaProducer, excepHandlers);
		} else if (List.class.isAssignableFrom(paramClazz)) {
			beanMessageHandler = createBeansHandler(kafkaHandlerMeta,
					kafkaProducer, excepHandlers);
		} else {
			beanMessageHandler = createBeanHandler(kafkaHandlerMeta,
					kafkaProducer, excepHandlers);
		}

		KafkaConsumer kafkaConsumer = createConsumer(kafkaHandlerMeta,
				beanMessageHandler);
		kafkaConsumer.startup();

		KafkaHandler kafkaHandler = new KafkaHandler(kafkaConsumer,
				kafkaProducer, excepHandlers, kafkaHandlerMeta);

		kafkaHandlers.add(kafkaHandler);

	}

	private List<ExceptionHandler> createExceptionHandlers(
			final KafkaHandlerMeta kafkaHandlerMeta) {
		List<ExceptionHandler> excepHandlers = new ArrayList<ExceptionHandler>();

		for (final Map.Entry<ErrorHandler, Method> errorHandler : kafkaHandlerMeta
				.getErrorHandlers().entrySet()) {
			ExceptionHandler exceptionHandler = new ExceptionHandler() {
				public boolean support(Throwable t) {
					// We handle the exception when the classes are exactly same
					return errorHandler.getKey().exception() == t.getClass();
				}

				public void handle(Throwable t, String message) {

					Method excepHandlerMethod = errorHandler.getValue();
					try {
						excepHandlerMethod.invoke(kafkaHandlerMeta.getBean(),
								t, message);

					} catch (IllegalAccessException e) {
						// If annotated exception handler is correct, this won't
						// happen
						log.error(
								"No permission to access the annotated exception handler.",
								e);
						throw new IllegalStateException(
								"No permission to access the annotated exception handler. Please check annotated config.",
								e);
					} catch (IllegalArgumentException e) {
						// If annotated exception handler is correct, this won't
						// happen
						log.error(
								"The parameter passed in doesn't match the annotated exception handler's.",
								e);
						throw new IllegalStateException(
								"The parameter passed in doesn't match the annotated exception handler's. Please check annotated config.",
								e);
					} catch (InvocationTargetException e) {
						// If the exception during handling exception occurs,
						// throw it, in SafelyMessageHandler, this will be
						// processed
						log.error(
								"Failed to call the annotated exception handler.",
								e);
						throw new IllegalStateException(
								"Failed to call the annotated exception handler. Please check if the handler can handle the biz without any exception.",
								e);
					}
				}
			};

			excepHandlers.add(exceptionHandler);
		}

		return excepHandlers;
	}

	protected ObjectMessageHandler<JSONObject> createObjectHandler(
			final KafkaHandlerMeta kafkaHandlerMeta,
			final KafkaProducer kafkaProducer,
			List<ExceptionHandler> excepHandlers) {

		ObjectMessageHandler<JSONObject> objectMessageHandler = new ObjectMessageHandler<JSONObject>(
				excepHandlers) {
			@Override
			protected void doExecuteObject(JSONObject jsonObject) {
				invokeHandler(kafkaHandlerMeta, kafkaProducer, jsonObject);
			}

		};

		return objectMessageHandler;
	}

	protected ObjectsMessageHandler<JSONArray> createObjectsHandler(
			final KafkaHandlerMeta kafkaHandlerMeta,
			final KafkaProducer kafkaProducer,
			List<ExceptionHandler> excepHandlers) {

		ObjectsMessageHandler<JSONArray> objectMessageHandler = new ObjectsMessageHandler<JSONArray>(
				excepHandlers) {
			@Override
			protected void doExecuteObjects(JSONArray jsonArray) {
				invokeHandler(kafkaHandlerMeta, kafkaProducer, jsonArray);
			}
		};

		return objectMessageHandler;
	}

	protected DocumentMessageHandler createDocumentHandler(
			final KafkaHandlerMeta kafkaHandlerMeta,
			final KafkaProducer kafkaProducer,
			List<ExceptionHandler> excepHandlers) {

		DocumentMessageHandler documentMessageHandler = new DocumentMessageHandler(
				excepHandlers) {
			@Override
			protected void doExecuteDocument(Document document) {
				invokeHandler(kafkaHandlerMeta, kafkaProducer, document);
			}
		};

		return documentMessageHandler;
	}

	@SuppressWarnings("unchecked")
	protected BeanMessageHandler<Object> createBeanHandler(
			final KafkaHandlerMeta kafkaHandlerMeta,
			final KafkaProducer kafkaProducer,
			List<ExceptionHandler> excepHandlers) {

		// We have to abandon the type check
		@SuppressWarnings("rawtypes")
		BeanMessageHandler beanMessageHandler = new BeanMessageHandler(
				kafkaHandlerMeta.getParameterType(), excepHandlers) {
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
			final KafkaProducer kafkaProducer,
			List<ExceptionHandler> excepHandlers) {

		// We have to abandon the type check
		@SuppressWarnings("rawtypes")
		BeansMessageHandler beanMessageHandler = new BeansMessageHandler(
				kafkaHandlerMeta.getParameterType(), excepHandlers) {
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
				if (result instanceof JSONObject)
					kafkaProducer.send(((JSONObject) result).toJSONString());
				else if (result instanceof JSONArray)
					kafkaProducer.send(((JSONArray) result).toJSONString());
				else if (result instanceof Document)
					kafkaProducer.send(((Document) result).getTextContent());
				else
					kafkaProducer.send(JSON.toJSONString(result));
			}
		} catch (IllegalAccessException e) {
			// If annotated config is correct, this won't happen
			log.error("No permission to access the annotated kafka handler.", e);
			throw new IllegalStateException(
					"No permission to access the annotated kafka handler. Please check annotated config.",
					e);
		} catch (IllegalArgumentException e) {
			// If annotated config is correct, this won't happen
			log.error(
					"The parameter passed in doesn't match the annotated kafka handler's.",
					e);
			throw new IllegalStateException(
					"The parameter passed in doesn't match the annotated kafka handler's. Please check annotated config.",
					e);
		} catch (InvocationTargetException e) {
			// The SafeMessageHanlder has already handled the
			// throwable, no more exception goes here
			log.error("Failed to call the annotated kafka handler.", e);
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

		} else if (kafkaHandlerMeta.getInputConsumer().maxThreadNum() > 0
				&& kafkaHandlerMeta.getInputConsumer().minThreadNum() < kafkaHandlerMeta
						.getInputConsumer().maxThreadNum()) {
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

		return kafkaConsumer;
	}

	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = applicationContext;
	}

	// The consumers can't be shutdown automatically by jvm shutdown hook if
	// this method is not called
	public void shutdownAll() {
		for (KafkaHandler kafkahandler : kafkaHandlers) {
			kafkahandler.getKafkaConsumer().shutdownGracefully();

			kafkahandler.getKafkaProducer().close();
		}
	}

	public List<KafkaHandler> getKafkaHandlers() {
		return kafkaHandlers;
	}

	public void setKafkaHandlers(List<KafkaHandler> kafkaHandlers) {
		this.kafkaHandlers = kafkaHandlers;
	}
}
