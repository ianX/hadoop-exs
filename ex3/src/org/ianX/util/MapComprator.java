package org.ianX.util;

import java.util.Comparator;
import java.util.Map;

public class MapComprator<K, V extends Comparable<V>> implements Comparator<K> {

	private Map<K, V> map;

	public MapComprator(Map<K, V> m) {
		// TODO Auto-generated constructor stub
		map = m;
	}

	@Override
	public int compare(K o1, K o2) {
		// TODO Auto-generated method stub
		return map.get(o1).compareTo(map.get(o2));
	}
	
	public V get(K k){
		return map.get(k);
	}

}
