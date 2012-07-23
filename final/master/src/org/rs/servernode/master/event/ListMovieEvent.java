package org.rs.servernode.master.event;

import java.util.EventObject;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.server.io.DataGetter;

public class ListMovieEvent extends EventObject {

	private static final long serialVersionUID = 8630204336073340717L;

	private DataGetter db;

	private Vector<Movie> ret;

	public ListMovieEvent(DataGetter source, Vector<Movie> ret) {
		super(source);
		this.db = source;
		this.ret = ret;
	}

	public DataGetter getDb() {
		return db;
	}

	public Vector<Movie> getRet() {
		return ret;
	}
}
