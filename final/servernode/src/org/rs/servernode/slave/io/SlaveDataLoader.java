package org.rs.servernode.slave.io;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;

/** MSK */
public class SlaveDataLoader implements DataLoader {

	private List<Movie> movieList = new Vector<Movie>();
	private List<Movie> mrec = new Vector<Movie>();
	private List<User> urec = new Vector<User>();

	public SlaveDataLoader() {
		// TODO Auto-generated constructor stub
		movieList.add(new Movie("kill bill"));
		movieList.add(new Movie("kill bill 2"));
		mrec.add(new Movie("kill bill 3"));
		urec.add(new User("bill", 0));
	}

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
		ret.addAll(mrec);
	}

	@Override
	public void getRecUser(List<Double> userVector, List<User> ret) {
		// TODO Auto-generated method stub
		ret.addAll(urec);
	}

	@Override
	public void getMovieList(List<Movie> ret) {
		// TODO Auto-generated method stub
		ret.addAll(movieList);
	}

}
