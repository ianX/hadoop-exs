package org.rs.client;

import java.util.List;
import org.rs.object.Movie;
import org.rs.object.User;

public interface ClientHandlerInterface {
	public int connect(String host, int port);

	public int close();

	public int addRating(Movie movie, int rating);

	public List<Movie> getMovieList();

	public List<Movie> getMovieList(List<Movie> list);

	public List<Movie> getRecMovie();

	public List<Movie> getRecMovie(List<Movie> list);

	public List<User> getRecUser();

	public List<User> getRecUser(List<User> list);
}
