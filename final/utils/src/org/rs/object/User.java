package org.rs.object;

import java.io.Serializable;

public class User implements Serializable, Comparable<User> {
	private static final long serialVersionUID = -7010197333489417337L;
	private String name = null;
	private int uid;
	private double sim = 0;

	public User(String name, int uid) {
		this.name = name;
		this.uid = uid;
	}

	public User(int uid) {
		this.uid = uid;
	}
	
	public User(int uid , double sim){
		this.uid = uid;
		this.sim = sim;
	}

	public int getUid() {
		return uid;
	}

	public void setSim(double sim) {
		this.sim = sim;
	}

	public double getSim() {
		return sim;
	}

	@Override
	public boolean equals(Object obj) {
		return ((User) obj).getUid() == uid;
	}

	@Override
	public int hashCode() {
		return Integer.valueOf(uid).hashCode();
	}

	@Override
	public String toString() {
		if (name == null)
			return "UID:" + Integer.toString(uid);
		else
			return name;
	}

	@Override
	public int compareTo(User o) {
		double diff = this.getSim() - o.getSim();
		return diff > 0 ? 1 : (diff < 0 ? -1 : 0);
	}
}
