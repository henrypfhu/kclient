package com.robert.kafka.kclient.boot;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.stereotype.Component;

/**
 * This annotation is used to declare a class to be a message handler
 * collection. This bean be also Spring @Component so that it can be
 * component-scanned by spring context.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface KafkaHandlers {
}
