package com.robert.kafka.kclient.reflection.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public interface AnnotationHandler<C, K, V> {
	public void handleMethodAnnotation(Class<? extends Object> clazz,
			Method method, Annotation annotation,
			TranversorContext<C, K, V> context);

	public void handleClassAnnotation(Class<? extends Object> clazz,
			Annotation annotation, TranversorContext<C, K, V> context);
}
