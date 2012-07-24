package org.rs.server.io;

import java.util.Map;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.servernode.master.event.DTEventListenser;

public class MasterDataGetter extends DataGetter {

	public MasterDataGetter(DTEventListenser listener) {
		this.addDBEventListenser(listener);
		new Thread(listener).start();
	}

	@Override
	public Vector<Double> getMovieVector(Map<Movie, Integer> urating,
			Vector<Double> ret) {
		this.notifyRatingEventListenser(urating, ret, true);
		return ret;
	}

	@Override
	public Vector<Double> getUserVector(Map<Movie, Integer> urating,
			Vector<Double> ret) {
		this.notifyRatingEventListenser(urating, ret, false);
		return ret;
	}

	@Override
	public Vector<Movie> getMovieList(Vector<Movie> ret) {
		System.out.println("begin get movie list");
		this.notifyListMovieEventListenser(ret);
		System.out.println("end get movie list");
		return ret;
	}

	@Override
	public Vector<Movie> getRecMovie(Vector<Double> movieVector,
			Vector<Movie> ret) {
		this.notifyRecMovieEventListenser(movieVector, ret);
		return ret;
	}

	@Override
	public Vector<User> getRecUser(Vector<Double> userVector, Vector<User> ret) {
		this.notifyRecUserEventListenser(userVector, ret);
		return ret;
	}

}
