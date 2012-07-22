package org.rs.servernode.master.event;

import java.util.EventObject;
import java.util.Vector;

import org.rs.object.User;
import org.rs.servernode.io.Database;

public class RecUserEvent extends EventObject {

	private static final long serialVersionUID = -1448038617334280965L;
	private Database db;
	private Vector<Double> userVector;
	private Vector<User> ret;

	public RecUserEvent(Database source, Vector<Double> userVector,
			Vector<User> ret) {
		super(source);
		// TODO Auto-generated constructor stub
		this.db = source;
		this.userVector = userVector;
		this.ret = ret;
	}

	public Database getDb() {
		return db;
	}

	public Vector<Double> getUserVector() {
		return userVector;
	}

	public Vector<User> getRet() {
		return ret;
	}

}
