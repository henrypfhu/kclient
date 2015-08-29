package com.robert.kafka.kclient.reflection.util;

import java.util.HashMap;
import java.util.Map;

public class TranversorContext<C, K, V> {

	private Map<C, Map<K, V>> data = new HashMap<C, Map<K, V>>();

	public void addEntry(C cat, K key, V value) {
		Map<K, V> map = data.get(cat);
		if (map == null) {
			map = new HashMap<K, V>();
			data.put(cat, map);
		}

		map.put(key, value);
	}

	public Map<C, Map<K, V>> getData() {
		return data;
	}
}
