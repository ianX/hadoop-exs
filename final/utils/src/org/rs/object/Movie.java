package org.rs.object;

import java.io.Serializable;

public class Movie implements Serializable {
	private static final long serialVersionUID = -4543712156703334945L;
	private String name;
	private int mid = -1;

	public Movie(String name) {
		this.name = name;
	}

	public Movie(String name, int mid) {
		// TODO Auto-generated constructor stub
		this.name = name;
		this.mid = mid;
	}

	public String getName() {
		return name;
	}

	public int getMid() {
		return mid;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		Movie m = (Movie) obj;
		return m.getMid() == mid || name.equals(m.getName());
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return name.hashCode();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return name;
	}
}
