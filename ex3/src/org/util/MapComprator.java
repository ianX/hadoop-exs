package org.util;

import java.util.Comparator;
import java.util.Map;

public class MapComprator<K, V> implements Comparator<K> {

	private Map<K, V> map;

	public MapComprator(Map<K, V> m) {
		// TODO Auto-generated constructor stub
		map = m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compare(K o1, K o2) {
		// TODO Auto-generated method stub
		return ((Comparable<V>)(map.get(o1))).compareTo(map.get(o2));
	}

}
