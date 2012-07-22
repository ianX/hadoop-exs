package org.rs.servernode.master;

import java.util.Map;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.servernode.master.event.HeartBeatEvent;
import org.rs.servernode.master.event.ListMovieEvent;
import org.rs.servernode.master.event.RatingEvent;
import org.rs.servernode.master.event.RecMovieEvent;
import org.rs.servernode.master.event.RecUserEvent;
import org.rs.servernode.protocol.SSPServer;

public class ServerMaster implements Master {

	private HeartBeat heartbeat;

	private boolean mUpdated = false;
	private Vector<Movie> movieList;
	private boolean rmUpdated = false;
	private Vector<Movie> recMovie;
	private boolean ruUpdated = false;
	private Vector<User> recUser;

	public ServerMaster() {
		// TODO Auto-generated constructor stub
		this.movieList = new Vector<Movie>();
		this.recMovie = new Vector<Movie>();
		this.recUser = new Vector<User>();
		this.movieList.add(new Movie("kill bill", 1));
		this.movieList.add(new Movie("kill bill II", 2));
		this.recMovie.add(new Movie("kill bill III", 3));
		this.recUser.add(new User("bill", 3));
	}

	@Override
	public void startHeartBeat() {
		// TODO Auto-generated method stub
		heartbeat = new HeartBeat();
		heartbeat.addHeartBeatEventListener(this);
		new Thread(heartbeat).start();
	}

	@Override
	public int InitAssemblage() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void handleHeartBeatEvent(HeartBeatEvent e) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleRatingEvent(RatingEvent event) {
		// TODO Auto-generated method stub
		if (event.isMovie()) {
			this.getMovieVector(event.getUrating(), event.getRet());
		} else {
			this.getUserVector(event.getUrating(), event.getRet());
		}
	}

	@Override
	public void handleListMovieEvent(ListMovieEvent event) {
		// TODO Auto-generated method stub
		this.getMovieList(event.getRet());
	}

	@Override
	public void handleRecMovieEvent(RecMovieEvent event) {
		// TODO Auto-generated method stub
		this.getRecMovie(event.getMovieVector(), event.getRet());
	}

	@Override
	public void handleRecUserEvent(RecUserEvent event) {
		// TODO Auto-generated method stub
		this.getRecUser(event.getUserVector(), event.getRet());
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public void getMovieVector(Map<Movie, Integer> urating, Vector<Double> ret) {
		// TODO Auto-generated method stub
	}

	public void getUserVector(Map<Movie, Integer> urating, Vector<Double> ret) {
		// TODO Auto-generated method stub
	}

	public void getMovieList(Vector<Movie> ret) {
		// TODO Auto-generated method stub
		ret.clear();
		ret.addAll(movieList);
	}

	public void getRecMovie(Vector<Double> movieVector, Vector<Movie> ret) {
		// TODO Auto-generated method stub
		ret.clear();
		ret.addAll(recMovie);
	}

	public void getRecUser(Vector<Double> userVector, Vector<User> ret) {
		// TODO Auto-generated method stub
		ret.clear();
		ret.addAll(recUser);
	}
}
