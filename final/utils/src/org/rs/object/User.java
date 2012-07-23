package org.rs.object;

import java.io.Serializable;

public class User implements Serializable, Comparable<User> {

	private static final long serialVersionUID = 8283243003197071899L;
	private String name = null;
	private int uid;
	private int marking = 0;

	public User(String name, int uid) {
		this.name = name;
		this.uid = uid;
	}

	public User(int uid) {
		this.uid = uid;
	}

	public void setMarking(int marking) {
		this.marking = marking;
	}

	public int getMarking() {
		return marking;
	}

	public int getUid() {
		return uid;
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
		return this.getMarking() - o.getMarking();
	}
}
