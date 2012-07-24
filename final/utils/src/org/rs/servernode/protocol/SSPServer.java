package org.rs.servernode.protocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;

public class SSPServer {

	private Socket socket;
	private String host;

	private ObjectOutputStream oos;
	private ObjectInputStream ois;

	public SSPServer(String host) {
		this.host = host;
	}

	public int connect() {
		try {
			socket = new Socket(host, Properties.SLAVE_PORT);
			oos = new ObjectOutputStream(socket.getOutputStream());
			ois = new ObjectInputStream(socket.getInputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	public int getVector(Map<Movie, Integer> urating, Vector<Double> ret,
			boolean isMovie) {
		int retval = -1;
		int cmd = isMovie ? Properties.MOVIE_VECTOR : Properties.USER_VECTOR;
		try {
			oos.writeInt(cmd);
			oos.writeObject(urating);
			oos.flush();
			ret.addAll((Vector<Double>) ois.readObject());
			retval = ois.readInt();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return retval;
	}

	@SuppressWarnings("unchecked")
	public void getMovieList(Vector<Movie> ret) {
		try {
			oos.writeInt(Properties.MOVIE_LIST);
			oos.flush();
			ret.addAll((Vector<Movie>) ois.readObject());
			System.out.println("SSPServer: " + ret.size());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public void getRecMovie(Vector<Double> movieVector, Vector<Movie> ret) {
		try {
			oos.writeInt(Properties.REC_MOVIE);
			oos.writeObject(movieVector);
			oos.flush();
			ret.addAll((Vector<Movie>) ois.readObject());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public void getRecUser(Vector<Double> userVector, Vector<User> ret) {
		try {
			oos.writeInt(Properties.REC_USER);
			oos.writeObject(userVector);
			oos.flush();
			ret.addAll((Vector<User>) ois.readObject());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
