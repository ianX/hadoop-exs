package org.rs.servernode.master.event;

import java.util.EventObject;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.servernode.io.Database;

public class ListMovieEvent extends EventObject {

	private static final long serialVersionUID = 8630204336073340717L;

	private Database db;

	private Vector<Movie> ret;

	public ListMovieEvent(Database source, Vector<Movie> ret) {
		super(source);
		// TODO Auto-generated constructor stub
		this.db = source;
		this.ret = ret;
	}

	public Database getDb() {
		return db;
	}

	public Vector<Movie> getRet() {
		return ret;
	}
}