package com.robert.kafka.kclient.reflection.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;

public class AnnotationTranversor<T, C, K, V> {
	Class<T> clazz;

	public AnnotationTranversor(Class<T> clazz) {
		this.clazz = clazz;
	}

	public Map<C, Map<K, V>> tranverseAnnotation(
			AnnotationHandler<T, C, K, V> annotationHandler) {
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
