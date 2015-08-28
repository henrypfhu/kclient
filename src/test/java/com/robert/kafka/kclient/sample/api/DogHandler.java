package com.robert.kafka.kclient.sample.api;

import com.robert.kafka.kclient.handlers.BeanMessageHandler;
import com.robert.kafka.kclient.sample.domain.Dog;

/**
 * The message handler sample to handle the actual beans.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class DogHandler extends BeanMessageHandler<Dog> {
	public DogHandler() {
		super(Dog.class);
	}

	protected void doExecuteBean(Dog dog) {
		System.out.format("Receiving dog: %s\n", dog);
	}
}
