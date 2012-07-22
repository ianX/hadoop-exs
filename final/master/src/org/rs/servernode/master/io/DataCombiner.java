package org.rs.servernode.master.io;

import java.util.List;

import org.rs.object.Movie;
import org.rs.object.User;

public interface DataCombiner {
	public void combineMovieVector(List<List<Double>> movieVectors,
			List<Integer> counts, List<Double> ret);

	public void combineUserVector(List<List<Double>> movieVectors,
			List<Integer> counts, List<Double> ret);

	public void combineRecMovie(List<List<Movie>> recMovies, List<Movie> ret);

	public void combineRecUser(List<List<User>> recUsers, List<User> ret);
}
