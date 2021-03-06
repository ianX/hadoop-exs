package org.rs.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.protocol.CSPServer;
import org.rs.protocol.Commands;
import org.rs.server.io.DataGetter;

public class ServerHandler implements ServerHandlerInterface {

	private CSPServer sprotocol = null;

	private DataGetter db = null;

	private Map<Movie, Integer> userRating = new HashMap<Movie, Integer>();
	private Vector<Double> movieVector = new Vector<Double>();
	private Vector<Double> userVector = new Vector<Double>();

	private Vector<Movie> movieList = new Vector<Movie>();
	private Vector<Movie> recMovie = new Vector<Movie>();
	private Vector<User> recUser = new Vector<User>();

	private Socket messager = null;
	private Socket datatransporter = null;

	private boolean dataSocketCreated = false;

	private BufferedWriter cmdWriter = null;
	private BufferedReader cmdReader = null;
	private ObjectOutputStream dataWriter = null;
	private ObjectInputStream dataReader = null;

	public ServerHandler(Socket messager, DataGetter db) throws IOException {
		sprotocol = new CSPServer();
		this.db = db;
		this.messager = messager;
		cmdWriter = new BufferedWriter(new OutputStreamWriter(
				messager.getOutputStream()));
		cmdReader = new BufferedReader(new InputStreamReader(
				messager.getInputStream()));
	}

	@Override
	public void run() {
		try {
			String cmd;
			while ((cmd = cmdReader.readLine()) != null) {
				System.out.println("cmd: " + cmd);
				RetCode cmdHandlerRetCode = cmdHandler(cmd);
				if (cmdHandlerRetCode.equals(RetCode.close)) {
					return;
				} else if (!cmdHandlerRetCode.equals(RetCode.good)) {
					RetCode errRetCode = cmdError(cmdHandlerRetCode);
					if (!errRetCode.equals(RetCode.good)) {
						this.close();
						return;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			this.close();
		}
	}

	@Override
	public RetCode createDataSocket() {
		try {
			datatransporter = sprotocol.getDataSocket(cmdWriter, cmdReader);

			dataWriter = new ObjectOutputStream(
					datatransporter.getOutputStream());

			dataReader = new ObjectInputStream(datatransporter.getInputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return RetCode.createDataSocketFailed;
		}
		return RetCode.good;
	}

	@Override
	public RetCode close() {
		try {
			this.dataWriter.close();
			this.dataReader.close();
			this.datatransporter.close();

			this.sprotocol.close(cmdWriter, cmdReader);
			this.cmdWriter.close();
			this.cmdReader.close();
			this.messager.close();
		} catch (IOException e) {
			e.printStackTrace();
			return RetCode.closeFailed;
		}
		return RetCode.close;
	}

	private void listFilter(List<Movie> list) {
		Iterator<Movie> iterator = list.iterator();
		while (iterator.hasNext()) {
			Movie m = iterator.next();
			System.out.println(m.toString());

			for (Movie movie : userRating.keySet()) {
				System.out.println(m.toString() + " " + movie.toString());
				if (m.equals(movie)) {
					System.out.println("remove");
					iterator.remove();
				}
			}

			// if (this.userRating.containsKey(m.getMid())) {
			// System.out.println("remove");
			// iterator.remove();
			// }
		}
	}

	@Override
	public RetCode sendMovieList() {
		// System.out.println("sending movie list");
		movieList.clear();
		db.getMovieList(movieList);
		listFilter(movieList);
		if (sprotocol.sendMovieList(movieList, cmdReader, cmdWriter,
				dataWriter, dataReader) != 0)
			return RetCode.sendMovieListFailed;
		return RetCode.good;
	}

	@Override
	public RetCode sendRecMovie() {
		recMovie.clear();
		db.getRecMovie(movieVector, recMovie);
		listFilter(recMovie);
		if (sprotocol.sendRecMovie(recMovie, cmdReader, cmdWriter, dataWriter,
				dataReader) != 0)
			return RetCode.sendRecMovieFailed;
		return RetCode.good;
	}

	@Override
	public RetCode sendRecUser() {
		recUser.clear();
		if (sprotocol.sendRecUser(db.getRecUser(userVector, recUser),
				cmdReader, cmdWriter, dataWriter, dataReader) != 0)
			return RetCode.sendRecUserFailed;
		return RetCode.good;
	}

	@Override
	public RetCode receiveRating() {
		if (sprotocol.receiveRating(userRating, cmdReader, cmdWriter,
				dataWriter, dataReader) != 0)
			return RetCode.receiveRatingFailed;
		System.out.println("------------------------------------------");
		for (Movie m : userRating.keySet()) {
			System.out.println(m.toString() + " " + userRating.get(m));
		}
		System.out.println("------------------------------------------");
		movieVector = db.getMovieVector(userRating, movieVector);
		userVector = db.getUserVector(userRating, userVector);
		System.out.println("ServerHandler: " + movieVector.size()
				+ userVector.size());
		return RetCode.good;
	}

	@Override
	public RetCode cmdHandler(String cmd) {
		if (cmd.equals(Commands.GET_DATA_PORT)) {
			RetCode createDataSocketRetCode = createDataSocket();
			if (!createDataSocketRetCode.equals(RetCode.good)) {
				this.close();
				return RetCode.createDataSocketFailed;
			}
			this.dataSocketCreated = true;
			return createDataSocketRetCode;
		} else if (this.dataSocketCreated) {
			if (cmd.equals(Commands.CLIENT_CLOSE)) {
				return close();
			} else if (cmd.equals(Commands.LIST_MOVIE)) {
				return sendMovieList();
			} else if (cmd.equals(Commands.REC_MOVIE)) {
				return sendRecMovie();
			} else if (cmd.equals(Commands.REC_USER)) {
				return sendRecUser();
			} else if (cmd.equals(Commands.RATING)) {
				return receiveRating();
			}
		}
		return RetCode.cmdError;
	}

	@Override
	public RetCode cmdError(RetCode errCode) {
		// TODO Auto-generated method stub
		return RetCode.good;
	}

}
