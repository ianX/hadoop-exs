package org.rs.client;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.rs.client.event.UIEvent;
import org.rs.client.event.UIEventListener;
import org.rs.client.event.UIRatingEvent;
import org.rs.client.ui.*;
import org.rs.object.Movie;
import org.rs.object.User;

public class Client implements UIEventListener {

	public class UIEventHandler implements Runnable {

		private UIEvent event;
		private UIRatingEvent revent;

		public UIEventHandler(UIEvent event) {
			synchronized (Client.this) {
				this.event = event;
				this.revent = null;
			}
		}

		public UIEventHandler(UIRatingEvent e) {
			synchronized (Client.this) {
				this.revent = e;
				this.event = null;
			}
		}

		@Override
		public void run() {
			synchronized (Client.this) {
				if (event != null)
					switch (event.getEventType()) {
					case CONNECT:
						connect(event);
						break;
					case COLSE:
						close(event.getSource());
						break;
					case RATING:
						addRating(event);
						break;
					case LIST_MOVIE:
						listMovie(event.getSource());
						break;
					case REC_MOVIE:
						listRecMovie(event.getSource());
						break;
					case REC_USER:
						listRecUser(event.getSource());
						break;
					default:
					}
				else if (revent != null) {
					addRating(revent);
				}
			}
		}
	}

	private Set<Movie> movieList = new HashSet<Movie>();
	private List<Movie> recMovie = new Vector<Movie>();
	private List<User> recUser = new Vector<User>();

	private ClientHandlerInterface handler;
	private String host;
	private int port;

	private boolean connected = false;

	private String errMess = "";

	private final static String DEFAULT_HOST = "localhost";
	private final static int DEFAULT_PORT = 6000;

	private Client(ClientHandlerInterface handler) {
		this.handler = handler;
	}

	private void connect(UIEvent event) {
		UI ui = event.getSource();
		String[] params = event.getParams();
		if (params != null) {
			if (!parseArgs(params)) {
				ui.printErrMessage(errMess);
				return;
			}
		}
		int ret = handler.connect(host, port);
		if (ret == 0) {
			this.connected = true;
			ui.printConnectMessage("connect success");
		} else {
			ui.printConnectMessage("connect failed");
		}
	}

	private void close(UI ui) {
		if (!this.connected) {
			System.exit(0);
		}
		handler.close();
		ui.printCloseMessage("now closing");
		ui.clean();
		System.exit(0);
	}

	private void listMovie(UI ui) {
		if (!this.connected)
			this.notConnected(ui);
		List<Movie> list = handler.getMovieList();
		movieList.addAll(list);
		ui.printMovieList(list);
	}

	private void addRating(UIRatingEvent event) {
		UI ui = event.getUi();
		try {
			Movie movie = event.getMovie();
			if (!this.movieList.contains(movie))
				throw new Exception("movie not exist!");
			handler.addRating(movie, event.getRating());
			this.listRecMovie(ui);
			this.listRecUser(ui);
		} catch (Exception e) {
			errMess = e.getMessage();
			ui.printErrMessage(errMess);
		}
	}

	private void addRating(UIEvent event) {
		UI ui = event.getSource();
		if (!this.connected)
			this.notConnected(ui);
		String[] params = event.getParams();
		if (params != null) {
			try {
				String movieName = params[0];
				int rating = Integer.parseInt(params[1]);
				if (rating < 0 || rating > 5)
					throw new NumberFormatException("port not in [0,5]!");
				Movie movie = new Movie(movieName);
				if (!this.movieList.contains(movie))
					throw new Exception("movie not exist!");
				handler.addRating(movie, rating);
				this.listRecMovie(ui);
				this.listRecUser(ui);
			} catch (NumberFormatException e) {
				errMess = e.getMessage();
				if (errMess.length() == 0)
					errMess = "wrong rating format!";
				ui.printErrMessage(errMess);
			} catch (Exception e) {
				errMess = e.getMessage();
				ui.printErrMessage(errMess);
			}
		}
	}

	private void listRecMovie(UI ui) {
		if (!this.connected)
			this.notConnected(ui);
		recMovie = handler.getRecMovie(recMovie);
		ui.printRecMovie(recMovie);
	}

	private void listRecUser(UI ui) {
		if (!this.connected)
			this.notConnected(ui);
		recUser = handler.getRecUser(recUser);
		ui.printRecUser(recUser);
	}

	private void notConnected(UI ui) {
		ui.printErrMessage("server not connected!");
	}

	@Override
	public void handleUIEvent(UIEvent e) {
		new Thread(new UIEventHandler(e)).start();
	}

	@Override
	public void handleUIRatingEvent(UIRatingEvent e) {
		new Thread(new UIEventHandler(e)).start();
	}

	private long sleepTime = 100000;

	private void start() {
		while (true) {
			try {
				Thread.sleep(sleepTime);
				// do sth
			} catch (InterruptedException e) {
			}
		}
	}

	private boolean parseArgs(String[] args) {
		try {
			if (args.length == 0) {
				host = DEFAULT_HOST;
				port = DEFAULT_PORT;
			} else if (args.length != 2) {
				throw new Exception("usage:java -jar client [host port]");
			} else {
				host = args[0];
				port = Integer.parseInt(args[1]);
				if (port <= 1024 || port > 65535)
					throw new NumberFormatException("port not in [1025,65535]!");
			}
		} catch (NumberFormatException e) {
			errMess = e.getMessage();
			if (errMess.length() == 0)
				errMess = "wrong number format!";
			return false;
		} catch (Exception e) {
			errMess = e.getMessage();
			return false;
		}
		return true;
	}

	private void printErrAndExit() {
		System.err.println(errMess);
		System.exit(-1);
	}

	/**
	 * @param args
	 *            [0] host [1] port
	 */
	public static void main(String[] args) {
		Client client = new Client(new ClientHandler());
		if (client.parseArgs(args)) {
			GUI.addUIEventListener(client);
			new GUI.UIStarter().show();
			client.start();
		} else
			client.printErrAndExit();
	}
}
