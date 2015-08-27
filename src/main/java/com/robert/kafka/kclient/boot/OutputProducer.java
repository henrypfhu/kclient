package com.robert.kafka.kclient.boot;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used in method level. It implies that the method's result
 * will be sent to the other queue. It store the metadata of the queue.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OutputProducer {
	String propertiesFile() default "";

	String defaultTopic() default "";
}
