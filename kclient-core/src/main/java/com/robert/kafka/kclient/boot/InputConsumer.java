package com.robert.kafka.kclient.boot;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used for a method which serve as a message handler. It
 * includes the metadata for the input topic.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface InputConsumer {
	String propertiesFile() default "";

	String topic() default "";

	int streamNum() default 1;

	int fixedThreadNum() default 0;

	int minThreadNum() default 0;

	int maxThreadNum() default 0;
}
