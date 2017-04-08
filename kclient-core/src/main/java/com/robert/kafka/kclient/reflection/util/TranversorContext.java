package com.robert.kafka.kclient.reflection.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Used to collect the traversal result.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 * @param <C>
 *            category type for the data entry
 * @param <K>
 *            key type for the data entry
 * @param <V>
 *            value type for the date entry
 */

public class TranversorContext<C, K, V> {

	private Map<C, Map<K, V>> data = new HashMap<C, Map<K, V>>();

	public void addEntry(C cat, K key, V value) {
		Map<K, V> map = data.get(cat);
		if (map == null) {
			map = new HashMap<K, V>();
			data.put(cat, map);
		}

		if (map.containsKey(key))
			throw new IllegalArgumentException(String.format(
					"Duplicated Annotation {} in a single handler method {}.",
					value.getClass(), key));

		map.put(key, value);
	}

	public Map<C, Map<K, V>> getData() {
		return data;
	}
}
