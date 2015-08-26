package com.robert.kafka.kclient.sample;

import com.robert.kafka.kclient.handlers.BeanMessageHandler;

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

	protected void doExecuteBean(Dog bean) {
		System.out.println(bean);
	}
}
