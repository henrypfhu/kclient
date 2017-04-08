package com.robert.kafka.kclient.reflection.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * 
 * The class is used to process an annotation which is passed in by
 * AnnotationTranversor main class. The annotation handler will output the
 * 3-dimension map to context.
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
public interface AnnotationHandler<C, K, V> {
	public void handleMethodAnnotation(Class<? extends Object> clazz,
			Method method, Annotation annotation,
			TranversorContext<C, K, V> context);

	public void handleClassAnnotation(Class<? extends Object> clazz,
			Annotation annotation, TranversorContext<C, K, V> context);
}
