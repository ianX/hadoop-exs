package org.rs.client;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.rs.client.event.UIEvent;
import org.rs.client.event.UIEventListener;
import org.rs.client.ui.CLI;
import org.rs.client.ui.UI;
import org.rs.object.Movie;
import org.rs.object.User;

public class Client implements UIEventListener {

	public class UIEventHandler implements Runnable {

		private UIEvent event;

		public UIEventHandler(UIEvent event) {
			// TODO Auto-generated constructor stub
			this.event = event;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			synchronized (Client.this) {
				switch (event.getEventType()) {
				case CONNECT:
					connect(event);
					break;
				case COLSE:
					close();
					break;
				case RATING:
					addReating(event);
					break;
				case LIST_MOVIE:
					listMovie();
					break;
				case REC_MOVIE:
					listRecMovie();
					break;
				case REC_USER:
					listRecUser();
					break;
				default:
				}
			}
		}
	}

	private Set<Movie> movieList = new HashSet<Movie>();
	private List<Movie> recMovie = new Vector<Movie>();
	private List<User> recUser = new Vector<User>();

	private ClientHandlerInterface handler;
	private UI ui;
	private String host;
	private int port;

	private boolean connected = false;

	private String errMess = "";

	private final static String DEFAULT_HOST = "localhost";
	private final static int DEFAULT_PORT = 6000;

	private Client(ClientHandlerInterface handler, UI ui) {
		this.handler = handler;
		this.ui = ui;
	}

	private void connect(UIEvent event) {
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

	private void close() {
		if (!this.connected) {
			System.exit(0);
		}
		handler.close();
		ui.printCloseMessage("now closing");
		ui.clean();
		System.exit(0);
	}

	private void listMovie() {
		if (!this.connected)
			this.notConnected();
		List<Movie> list = handler.getMovieList();
		movieList.addAll(list);
		ui.printMovieList(list);
	}

	private void addReating(UIEvent event) {
		if (!this.connected)
			this.notConnected();
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
				this.listRecMovie();
				this.listRecUser();
			} catch (NumberFormatException e) {
				errMess = e.getMessage();
				if (errMess.length() == 0)
					errMess = "wrong rating format!";
				ui.printErrMessage(errMess);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				errMess = e.getMessage();
				ui.printErrMessage(errMess);
			}
		}
	}

	private void listRecMovie() {
		if (!this.connected)
			this.notConnected();
		recMovie = handler.getRecMovie(recMovie);
		ui.printRecMovie(recMovie);
	}

	private void listRecUser() {
		if (!this.connected)
			this.notConnected();
		recUser = handler.getRecUser(recUser);
		ui.printRecUser(recUser);
	}

	private void notConnected() {
		ui.printErrMessage("server not connected!");
	}

	@Override
	public void handleUIEvent(UIEvent e) {
		new Thread(new UIEventHandler(e)).start();
	}

	private long sleepTime = 100000;

	private void start() {
		ui.addUIEventListener(this);
		new Thread(ui).start();
		while (true) {
			if (this.connected) {
				synchronized (this) {
					ui.clear();
					this.listMovie();
					this.listRecMovie();
					this.listRecUser();
				}
			}
			try {
				Thread.sleep(sleepTime);
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
		ui.printErrMessage(errMess);
		System.exit(-1);
	}

	/**
	 * @param args
	 *            [0] host [1] port
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		UI ui = new CLI();
		Client client = new Client(new ClientHandler(), ui);
		if (client.parseArgs(args)) {
			client.start();
		} else
			client.printErrAndExit();
	}

}
