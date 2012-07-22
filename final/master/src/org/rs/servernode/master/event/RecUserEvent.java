package org.rs.servernode.master.event;

import java.util.EventObject;
import java.util.Vector;

import org.rs.object.User;
import org.rs.server.io.DataGetter;

public class RecUserEvent extends EventObject {

	private static final long serialVersionUID = -1448038617334280965L;
	private DataGetter db;
	private Vector<Double> userVector;
	private Vector<User> ret;

	public RecUserEvent(DataGetter source, Vector<Double> userVector,
			Vector<User> ret) {
		super(source);
		// TODO Auto-generated constructor stub
		this.db = source;
		this.userVector = userVector;
		this.ret = ret;
	}

	public DataGetter getDb() {
		return db;
	}

	public Vector<Double> getUserVector() {
		return userVector;
	}

	public Vector<User> getRet() {
		return ret;
	}

}
