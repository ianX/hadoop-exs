package org.rs.servernode.master.io;

import java.util.Map;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.servernode.master.event.DTEventListenser;
import org.rs.servernode.master.event.ListMovieEvent;
import org.rs.servernode.master.event.RatingEvent;
import org.rs.servernode.master.event.RecMovieEvent;
import org.rs.servernode.master.event.RecUserEvent;

public abstract class DataGetter {

	private final Vector<DTEventListenser> listeners = new Vector<DTEventListenser>();

	public final void addDBEventListenser(DTEventListenser listenser) {
		this.listeners.add(listenser);
	}

	public final void removeDBEventListenser(DTEventListenser listenser) {
		this.listeners.remove(listenser);
	}

	public final void notifyRatingEventListenser(Map<Movie, Integer> urating,
			Vector<Double> ret, boolean type) {
		for (DTEventListenser listenser : listeners) {
			listenser.handleRatingEvent(new RatingEvent(this, urating, ret,
					type));
		}
	}

	public final void notifyListMovieEventListenser(Vector<Movie> ret) {
		for (DTEventListenser listenser : listeners) {
			listenser.handleListMovieEvent(new ListMovieEvent(this, ret));
		}
	}

	public final void notifyRecMovieEventListenser(Vector<Double> movieVector,
			Vector<Movie> ret) {
		for (DTEventListenser listenser : listeners) {
			listenser.handleRecMovieEvent(new RecMovieEvent(this, movieVector,
					ret));
		}
	}

	public final void notifyRecUserEventListenser(Vector<Double> userVector,
			Vector<User> ret) {
		for (DTEventListenser listenser : listeners) {
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
