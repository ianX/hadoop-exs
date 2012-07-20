package org.rs.protocol;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.rs.object.Movie;
import org.rs.object.User;

public class CSPClient {

	private int retry = 10;

	public int getDataTransPort(BufferedReader cmdReader,
			BufferedWriter cmdWriter) {
		int port = 0;
		try {
			cmdWriter.write(Commands.GET_DATA_PORT);
			cmdWriter.newLine();
			cmdWriter.flush();
			String mess;
			if (!(mess = cmdReader.readLine()).equals(Commands.DATA_PORT)) {
				System.out.println(mess);
				cmdReader.readLine();
				return 0;
			}
			String portString = cmdReader.readLine();
			System.out.println(portString);
			port = Integer.parseInt(portString);
			if (port <= 0 || port > 65535) {
				throw new NumberFormatException("port error: " + port);
			}
			cmdWriter.append(Commands.DATA_PORT_RECEIVED);
			cmdWriter.newLine();
			cmdWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
		return port;
	}

	public int close(BufferedReader cmdReader, BufferedWriter cmdWriter) {
		for (int i = 0; i < retry; i++) {
			try {
				cmdWriter.write(Commands.CLIENT_CLOSE);
				cmdWriter.newLine();
				cmdWriter.flush();
				break;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	public int getMovieList(List<Movie> list, BufferedReader cmdReader,
			BufferedWriter cmdWriter, ObjectOutputStream dataWriter,
			ObjectInputStream dataReader) {
		try {
			cmdWriter.write(Commands.LIST_MOVIE);
			cmdWriter.newLine();
			cmdWriter.flush();
			String mess;
			if (!(mess = cmdReader.readLine())
					.equals(Commands.LIST_MOVIE_START)) {
				System.out.println("LIST_MOVIE_START error:" + mess);
				return -1;
			}

			list.addAll((List<Movie>) dataReader.readObject());

			if (!(mess = cmdReader.readLine()).equals(Commands.LIST_MOVIE_END)) {
				System.out.println("LIST_MOVIE_END error:" + mess);
				return -1;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	public int getRecMovie(List<Movie> list, BufferedReader cmdReader,
			BufferedWriter cmdWriter, ObjectOutputStream dataWriter,
			ObjectInputStream dataReader) {
		try {
			cmdWriter.write(Commands.REC_MOVIE);
			cmdWriter.newLine();
			cmdWriter.flush();
			String mess;
			if (!(mess = cmdReader.readLine()).equals(Commands.REC_MOVIE_START)) {
				System.out.println("REC_MOVIE_START error:" + mess);
				return -1;
			}

			list.addAll((List<Movie>) dataReader.readObject());

			if (!(mess = cmdReader.readLine()).equals(Commands.REC_MOVIE_END)) {
				System.out.println("REC_MOVIE_END error:" + mess);
				return -1;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	public int getRecUser(List<User> list, BufferedReader cmdReader,
			BufferedWriter cmdWriter, ObjectOutputStream dataWriter,
			ObjectInputStream dataReader) {
		try {
			cmdWriter.write(Commands.REC_USER);
			cmdWriter.newLine();
			cmdWriter.flush();
			String mess;
			if (!(mess = cmdReader.readLine()).equals(Commands.REC_USER_START)) {
				System.out.println("REC_USER_START error:" + mess);
				return -1;
			}

			list.addAll((List<User>) dataReader.readObject());

			if (!(mess = cmdReader.readLine()).equals(Commands.REC_USER_END)) {
				System.out.println("REC_USER_END error:" + mess);
				return -1;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public int addRating(Movie movie, int rate, BufferedReader cmdReader,
			BufferedWriter cmdWriter, ObjectOutputStream dataWriter,
			ObjectInputStream dataReader) {
		try {
			cmdWriter.write(Commands.RATING);
			cmdWriter.newLine();
			cmdWriter.flush();

			cmdWriter.write(Commands.RATING_START);
			cmdWriter.newLine();
			cmdWriter.flush();

			dataWriter.reset();
			dataWriter.writeObject(movie);
			dataWriter.writeInt(rate);
			dataWriter.flush();

			cmdWriter.write(Commands.RATING_END);
			cmdWriter.newLine();
			cmdWriter.flush();

			String mess;
			if (!(mess = cmdReader.readLine()).equals(Commands.RATING_RECEIVED)) {
				System.out.println(mess);
				return -1;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
}
