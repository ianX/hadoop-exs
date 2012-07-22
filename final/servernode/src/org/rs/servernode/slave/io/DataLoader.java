package org.rs.servernode.slave.io;

import java.util.List;
import java.util.Map;

import org.rs.object.Movie;
import org.rs.object.User;

public interface DataLoader {
	public int getMovieVector(Map<Movie, Integer> rating, List<Double> ret);

	public int getUserVector(Map<Movie, Integer> rating, List<Double> ret);

	public void getRecMovie(List<Double> movieVector, List<Movie> ret);

	public void getRecUser(List<Double> userVector, List<User> ret);
}
