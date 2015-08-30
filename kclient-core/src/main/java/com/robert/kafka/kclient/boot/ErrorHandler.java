package com.robert.kafka.kclient.boot;

public @interface ErrorHandler {
	Class<? extends Throwable> exception() default Throwable.class;

	String topic() default "";
}
