package org.rs.servernode.master.event;

import java.util.EventObject;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.servernode.master.io.DataGetter;

public class RecMovieEvent extends EventObject {

	private static final long serialVersionUID = 9155642645855618662L;
	private DataGetter db;
	private Vector<Double> movieVector;
	private Vector<Movie> ret;

	public RecMovieEvent(DataGetter source, Vector<Double> movieVector,
			Vector<Movie> ret) {
		super(source);
		// TODO Auto-generated constructor stub
		this.db = source;
		this.movieVector = movieVector;
		this.ret = ret;
	}

	public DataGetter getDb() {
		return db;
	}

	public Vector<Double> getMovieVector() {
		return movieVector;
	}

	public Vector<Movie> getRet() {
		return ret;
	}
}
