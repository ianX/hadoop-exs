package org.rs.servernode.master.event;

import java.util.EventObject;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.servernode.io.Database;

public class RecMovieEvent extends EventObject {

	private static final long serialVersionUID = 9155642645855618662L;
	private Database db;
	private Vector<Double> movieVector;
	private Vector<Movie> ret;

	public RecMovieEvent(Database source, Vector<Double> movieVector,
			Vector<Movie> ret) {
		super(source);
		// TODO Auto-generated constructor stub
		this.db = source;
		this.movieVector = movieVector;
		this.ret = ret;
	}

	public Database getDb() {
		return db;
	}

	public Vector<Double> getMovieVector() {
		return movieVector;
	}

	public Vector<Movie> getRet() {
		return ret;
	}
}
