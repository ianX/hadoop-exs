package org.rs.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.protocol.CSPClient;


public class ClientHandler implements ClientHandlerInterface {

	private CSPClient cprotocol;

	private int dataport = 0;
	private Socket messager = null;
	private Socket datatransporter = null;
	private BufferedWriter cmdWriter = null;
	private BufferedReader cmdReader = null;
	private ObjectOutputStream dataWriter = null;
	private ObjectInputStream dataReader = null;
	private Map<Movie, Integer> userRating = new HashMap<Movie, Integer>();

	public ClientHandler() {
		cprotocol = new CSPClient();
	}

	@Override
	public int connect(String host, int port) {
		try {
			messager = new Socket(host, port);
			cmdWriter = new BufferedWriter(new OutputStreamWriter(
					messager.getOutputStream()));
			cmdReader = new BufferedReader(new InputStreamReader(
					messager.getInputStream()));
			do {
				dataport = cprotocol.getDataTransPort(cmdReader, cmdWriter);
			} while (dataport == 0);

			datatransporter = new Socket(host, dataport);
			dataWriter = new ObjectOutputStream(
					datatransporter.getOutputStream());
			dataReader = new ObjectInputStream(datatransporter.getInputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	@Override
	public int close() {
		try {
			cprotocol.close(cmdReader, cmdWriter);
			this.dataReader.close();
			this.dataWriter.close();
			this.datatransporter.close();

			this.cmdReader.close();
			this.cmdWriter.close();
			this.messager.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public List<Movie> getMovieList() {
		return getMovieList(new ArrayList<Movie>());
	}

	@Override
	public List<Movie> getMovieList(List<Movie> list) {
		list.clear();
		cprotocol.getMovieList(list, cmdReader, cmdWriter, dataWriter,
				dataReader);
		return list;
	}

	@Override
	public List<Movie> getRecMovie() {
		return getMovieList(new ArrayList<Movie>());
	}

	@Override
	public List<Movie> getRecMovie(List<Movie> list) {
		list.clear();
		cprotocol.getRecMovie(list, cmdReader, cmdWriter, dataWriter,
				dataReader);
		return list;
	}

	@Override
	public List<User> getRecUser() {
		return getRecUser(new ArrayList<User>());
	}

	@Override
	public List<User> getRecUser(List<User> list) {
		list.clear();
		cprotocol
				.getRecUser(list, cmdReader, cmdWriter, dataWriter, dataReader);
		return list;
	}

	@Override
	public int addRating(Movie movie, int rating) {
		if (userRating.containsKey(movie))
			return -1;
		userRating.put(movie, rating);
		cprotocol.addRating(movie, rating, cmdReader, cmdWriter, dataWriter,
				dataReader);
		return 0;
	}

}
