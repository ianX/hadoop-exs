package org.rs.servernode.slave;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.servernode.protocol.Properties;
import org.rs.servernode.slave.io.SlaveDataLoader;

public class SlaveCmdHandler implements Runnable {

	private class CmdProcesser implements Runnable {

		private Socket socket;

		public CmdProcesser(Socket socket) {
			this.socket = socket;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			try {
				ObjectOutputStream oos = new ObjectOutputStream(
						socket.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(
						socket.getInputStream());
				int cmd = ois.readInt();
				switch (cmd) {
				case Properties.MOVIE_LIST:
					Vector<Movie> mList = new Vector<Movie>();
					dataLoader.getMovieList(mList);
					oos.writeObject(mList);
					break;
				case Properties.MOVIE_VECTOR:
					Map<Movie, Integer> rating4m = (HashMap<Movie, Integer>) ois
							.readObject();
					Vector<Double> mVector = new Vector<Double>();
					int mweight = dataLoader.getMovieVector(rating4m, mVector);
					oos.writeObject(mVector);
					oos.writeInt(mweight);
					break;
				case Properties.USER_VECTOR:
					Map<Movie, Integer> rating4u = (HashMap<Movie, Integer>) ois
							.readObject();
					Vector<Double> uVector = new Vector<Double>();
					int uweight = dataLoader.getUserVector(rating4u, uVector);
					oos.writeObject(uVector);
					oos.writeInt(uweight);
					break;
				case Properties.REC_MOVIE:
					Vector<Double> mvector = (Vector<Double>) ois.readObject();
					Vector<Movie> recMovie = new Vector<Movie>();
					dataLoader.getRecMovie(mvector, recMovie);
					oos.writeObject(recMovie);
					break;
				case Properties.REC_USER:
					Vector<Double> uvector = (Vector<Double>) ois.readObject();
					Vector<User> recUser = new Vector<User>();
					dataLoader.getRecUser(uvector, recUser);
					oos.writeObject(recUser);
					break;
				default:
				}
				oos.flush();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

	}

	private SlaveDataLoader dataLoader;

	private ServerSocket slaveSocket;

	public SlaveCmdHandler(SlaveDataLoader dataLoader) {
		this.dataLoader = dataLoader;
	}

	@Override
	public void run() {
		try {
			slaveSocket = new ServerSocket(Properties.SLAVE_PORT, 4000);
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("can't open slave port");
			System.exit(-1);
		}

		while (true) {
			try {
				Socket socket = slaveSocket.accept();
				new Thread(new CmdProcesser(socket)).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
