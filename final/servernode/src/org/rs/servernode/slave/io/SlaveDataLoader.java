package org.rs.servernode.slave.io;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.rs.object.Movie;
import org.rs.object.User;

/** MSK */
public class SlaveDataLoader implements DataLoader {

	@Override
	public void addFiles(Set<String> files, boolean isMovie) {
		// TODO Auto-generated method stub

	}

	@Override
	public void filesToRemove(Set<String> files) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeMarkedFiles() {
		// TODO Auto-generated method stub

	}

	@Override
	public int getMovieVector(Map<Movie, Integer> rating, List<Double> ret) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getUserVector(Map<Movie, Integer> rating, List<Double> ret) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void getRecMovie(List<Double> movieVector, List<Movie> ret) {
		// TODO Auto-generated method stub

	}

	@Override
	public void getRecUser(List<Double> userVector, List<User> ret) {
		// TODO Auto-generated method stub

	}

	@Override
	public void getMovieList(List<Movie> ret) {
		// TODO Auto-generated method stub

	}

}
