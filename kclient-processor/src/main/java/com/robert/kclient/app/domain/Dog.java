package com.robert.kclient.app.domain;

/**
 * Sample bean for conversion between JSON and object.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class Dog {
	private long id;

	private String name;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("Dog[");
		sb.append("id=").append(id).append(",");
		sb.append("name=").append(name).append("]");
		return sb.toString();
	}

};