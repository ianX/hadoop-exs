package org.rs.protocol;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.rs.object.Movie;
import org.rs.object.User;

public class CSPServer {
	private ServerSocket dataServer = null;

	private int port;

	private int retry = 10;

	private Random rand = new Random();

	public Socket getDataSocket(BufferedWriter cmdWriter,
			BufferedReader cmdReader) throws IOException {
		int count = 0;
		while (dataServer == null || count++ > retry) {
			try {
				port = rand.nextInt(55000) + 1024;
				dataServer = new ServerSocket(port);
				System.out.println("data port: " + port);
				break;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				dataServer = null;
			}
		}
		if (dataServer == null)
			throw new IOException();

		Socket dataSocket = null;

		cmdWriter.write(Commands.DATA_PORT);
		cmdWriter.newLine();
		cmdWriter.append(Integer.toString(port));
		cmdWriter.newLine();
		cmdWriter.flush();
		String mess;
		if (!(mess = cmdReader.readLine()).equals(Commands.DATA_PORT_RECEIVED)) {
			System.out.println(mess);
			throw new IOException();
		}

		dataSocket = dataServer.accept();
		return dataSocket;
	}

	public void close(BufferedWriter cmdWriter, BufferedReader cmdReader) {
		try {
			dataServer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int sendMovieList(List<Movie> list, BufferedReader cmdReader,
			BufferedWriter cmdWriter, ObjectOutputStream dataWriter,
			ObjectInputStream dataReader) {
		try {
			cmdWriter.write(Commands.LIST_MOVIE_START);
			cmdWriter.newLine();
			cmdWriter.flush();

			// System.out.println("starting");
			dataWriter.reset();
			dataWriter.writeObject(list);
			dataWriter.flush();

			// System.out.println("ending");
			cmdWriter.write(Commands.LIST_MOVIE_END);
			cmdWriter.newLine();
			cmdWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	public int sendRecMovie(List<Movie> list, BufferedReader cmdReader,
			BufferedWriter cmdWriter, ObjectOutputStream dataWriter,
			ObjectInputStream dataReader) {
		try {
			cmdWriter.write(Commands.REC_MOVIE_START);
			cmdWriter.newLine();
			cmdWriter.flush();

			// System.out.println("starting");
			dataWriter.reset();
			dataWriter.writeObject(list);
			dataWriter.flush();

			// System.out.println("ending");
			cmdWriter.write(Commands.REC_MOVIE_END);
			cmdWriter.newLine();
			cmdWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	public int sendRecUser(List<User> list, BufferedReader cmdReader,
			BufferedWriter cmdWriter, ObjectOutputStream dataWriter,
			ObjectInputStream dataReader) {
		try {
			cmdWriter.write(Commands.REC_USER_START);
			cmdWriter.newLine();
			cmdWriter.flush();

			// System.out.println("starting");
			dataWriter.reset();
			dataWriter.writeObject(list);
			dataWriter.flush();

			// System.out.println("ending");
			cmdWriter.write(Commands.REC_USER_END);
			cmdWriter.newLine();
			cmdWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	public int receiveRating(Map<Movie, Integer> userRating,
			BufferedReader cmdReader, BufferedWriter cmdWriter,
			ObjectOutputStream dataWriter, ObjectInputStream dataReader) {
		try {
			String mess;
			if (!(mess = cmdReader.readLine()).equals(Commands.RATING_START)) {
				System.out.println("RATING_START error:" + mess);
				return -1;
			}

			Movie movie = (Movie) dataReader.readObject();
			int rating = dataReader.readInt();
			userRating.put(movie, rating);

			if (!(mess = cmdReader.readLine()).equals(Commands.RATING_END)) {
				System.out.println("RATING_END error:" + mess);
				return -1;
			}

			cmdWriter.write(Commands.RATING_RECEIVED);
			cmdWriter.newLine();
			cmdWriter.flush();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
}
