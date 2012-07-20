package org.rs.servernode.master.event;

import java.util.EventObject;
import java.util.Map;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.servernode.io.Database;

public class RatingEvent extends EventObject {

	private static final long serialVersionUID = -4017421239472354022L;
	private Database db;
	private Map<Movie, Integer> urating;
	private Vector<Double> ret ;
	boolean isMovie;
	public RatingEvent(Database source ,Map<Movie, Integer> urating,
			Vector<Double> ret , boolean type) {
		super(source);
		// TODO Auto-generated constructor stub
		this.urating = urating;
		this.ret = ret;
		this.isMovie = type;
	}
	
	public Database getDb() {
		return db;
	}
	public Map<Movie, Integer> getUrating() {
		return urating;
	}
	public Vector<Double> getRet() {
		return ret;
	}
	public boolean isMovie() {
		return isMovie;
	}
}
