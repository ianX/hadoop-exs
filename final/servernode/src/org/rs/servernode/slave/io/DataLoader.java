package org.rs.servernode.slave.io;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.rs.object.Movie;
import org.rs.object.User;

public interface DataLoader {
	public void addFiles(Set<String> files, boolean isMovie);

	public void filesToRemove(Set<String> files);

	public void removeMarkedFiles();

	public void getMovieList(List<Movie> ret);

	public int getMovieVector(Map<Movie, Integer> rating, List<Double> ret);

	public int getUserVector(Map<Movie, Integer> rating, List<Double> ret);

	public void getRecMovie(List<Double> movieVector, List<Movie> ret);

	public void getRecUser(List<Double> userVector, List<User> ret);
}
