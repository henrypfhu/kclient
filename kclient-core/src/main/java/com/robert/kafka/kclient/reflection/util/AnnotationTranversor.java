package com.robert.kafka.kclient.reflection.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * This is the main class to traverse a class definition and provide any
 * declared annotation for AnnotationHandler.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 * @param <C>
 *            category type for the data entry
 * @param <K>
 *            key type for the data entry
 * @param <V>
 *            value type for the date entry
 */

public class AnnotationTranversor<C, K, V> {
	Class<? extends Object> clazz;

	public AnnotationTranversor(Class<? extends Object> clazz) {
		this.clazz = clazz;
	}

	public Map<C, Map<K, V>> tranverseAnnotation(
			AnnotationHandler<C, K, V> annotationHandler) {
		TranversorContext<C, K, V> ctx = new TranversorContext<C, K, V>();

		for (Annotation annotation : clazz.getAnnotations()) {
			annotationHandler.handleClassAnnotation(clazz, annotation, ctx);
		}

		for (Method method : clazz.getMethods()) {
			for (Annotation annotation : method.getAnnotations()) {
				annotationHandler.handleMethodAnnotation(clazz, method,
						annotation, ctx);
			}
		}

		return ctx.getData();
	}
}
