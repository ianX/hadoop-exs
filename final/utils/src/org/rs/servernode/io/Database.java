package org.rs.servernode.io;

import java.util.Map;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.servernode.master.event.DBEventListenser;
import org.rs.servernode.master.event.ListMovieEvent;
import org.rs.servernode.master.event.RatingEvent;
import org.rs.servernode.master.event.RecMovieEvent;
import org.rs.servernode.master.event.RecUserEvent;

public abstract class Database {

	private final Vector<DBEventListenser> listeners = new Vector<DBEventListenser>();

	public final void addDBEventListenser(DBEventListenser listenser) {
		this.listeners.add(listenser);
	}

	public final void removeDBEventListenser(DBEventListenser listenser) {
		this.listeners.remove(listenser);
	}

	public final void notifyRatingEventListenser(Map<Movie, Integer> urating,
			Vector<Double> ret, boolean type) {
		for (DBEventListenser listenser : listeners) {
			listenser.handleRatingEvent(new RatingEvent(this, urating, ret,
					type));
		}
	}

	public final void notifyListMovieEventListenser(Vector<Movie> ret) {
		for (DBEventListenser listenser : listeners) {
			listenser.handleListMovieEvent(new ListMovieEvent(this, ret));
		}
	}

	public final void notifyRecMovieEventListenser(Vector<Double> movieVector,
			Vector<Movie> ret) {
		for (DBEventListenser listenser : listeners) {
			listenser.handleRecMovieEvent(new RecMovieEvent(this, movieVector,
					ret));
		}
	}

	public final void notifyRecUserEventListenser(Vector<Double> userVector,
			Vector<User> ret) {
		for (DBEventListenser listenser : listeners) {
			listenser
					.handleRecUserEvent(new RecUserEvent(this, userVector, ret));
		}
	}

	public abstract Vector<Double> getMovieVector(Map<Movie, Integer> urating,
			Vector<Double> ret);

	public abstract Vector<Double> getUserVector(Map<Movie, Integer> urating,
			Vector<Double> ret);

	public abstract Vector<Movie> getMovieList(Vector<Movie> ret);

	public abstract Vector<Movie> getRecMovie(Vector<Double> movieVector,
			Vector<Movie> ret);

	public abstract Vector<User> getRecUser(Vector<Double> userVector,
			Vector<User> ret);
}
