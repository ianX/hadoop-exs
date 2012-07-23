package org.rs.servernode.protocol;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;

public class SSPServer {

	private Socket socket;
	private String host;

	public SSPServer(String host) {
		// TODO Auto-generated constructor stub
		this.host = host;
	}

	public int connect() {
		try {
			socket = new Socket(host, Properties.SLAVE_PORT);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	public void getVector(Map<Movie, Integer> urating, Vector<Double> ret,
			boolean isMovie) {
		// TODO Auto-generated method stub
	}

	public void getMovieList(Vector<Movie> ret) {
		// TODO Auto-generated method stub

	}

	public void getRecMovie(Vector<Double> movieVector, Vector<Movie> ret) {
		// TODO Auto-generated method stub

	}

	public void getRecUser(Vector<Double> userVector, Vector<User> ret) {
		// TODO Auto-generated method stub

	}
}
