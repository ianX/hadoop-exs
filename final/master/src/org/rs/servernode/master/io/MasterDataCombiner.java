package org.rs.servernode.master.io;

import java.util.List;

import org.rs.object.Movie;
import org.rs.object.User;

/**MSK*/
public class MasterDataCombiner implements DataCombiner {

	@Override
	public void combineMovieVector(List<List<Double>> movieVectors,
			List<Integer> counts, List<Double> ret) {
		// TODO Auto-generated method stub

	}

	@Override
	public void combineUserVector(List<List<Double>> movieVectors,
			List<Integer> counts, List<Double> ret) {
		// TODO Auto-generated method stub

	}

	@Override
	public void combineRecMovie(List<List<Movie>> recMovies, List<Movie> ret) {
		// TODO Auto-generated method stub

	}

	@Override
	public void combineRecUser(List<List<User>> recUsers, List<User> ret) {
		// TODO Auto-generated method stub

	}

}
