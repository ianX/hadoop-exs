package org.rs.object;

import java.io.Serializable;

public class User implements Serializable {

	private static final long serialVersionUID = 8283243003197071899L;
	private String name = null;
	private int uid;

	public User(String name, int uid) {
		// TODO Auto-generated constructor stub
		this.name = name;
		this.uid = uid;
	}

	public User(int uid) {
		// TODO Auto-generated constructor stub
		this.uid = uid;
	}

	public int getUid() {
		return uid;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return ((User) obj).getUid() == uid;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return Integer.valueOf(uid).hashCode();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		if (name == null)
			return "UID:" + Integer.toString(uid);
		else
			return name;
	}
}
