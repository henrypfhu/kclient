package com.robert.kafka.kclient.boot;

public @interface ExceptionHandler {
	Class<? extends Throwable> exception() default Throwable.class;
}
