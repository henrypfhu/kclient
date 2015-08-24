package com.robert.kafka.kclient.sample;

import com.robert.kafka.kclient.handlers.BeanMessageHandler;

/**
 * The message handler sample to handle the actual beans.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class MyBeanHandler extends BeanMessageHandler<MyBean> {
	public MyBeanHandler() {
		super(MyBean.class);
	}

	protected void doExecuteBean(MyBean bean) {
		System.out.println(bean);
	}
}
