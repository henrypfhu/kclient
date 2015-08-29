package com.robert.kafka.kclient.reflection.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public interface AnnotationHandler<T, C, K, V> {
	public void handleMethodAnnotation(Class<T> clazz, Method method,
			Annotation annotation, TranversorContext<C, K, V> context);

	public void handleClassAnnotation(Class<T> clazz, Annotation annotation,
			TranversorContext<C, K, V> context);
}
