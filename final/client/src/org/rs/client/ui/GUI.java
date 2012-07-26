package org.rs.client.ui;

import java.util.List;
import java.util.Vector;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.scene.Group;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.stage.Stage;

import org.rs.client.event.EventType;
import org.rs.client.ui.javafx.Login;
import org.rs.client.ui.javafx.MovieList;
import org.rs.client.ui.javafx.NextButton;
import org.rs.client.ui.javafx.RecMovieList;
import org.rs.client.ui.javafx.RecUserList;
import org.rs.client.ui.www.MovieDetailDetector;
import org.rs.object.Movie;
import org.rs.object.User;

public class GUI extends UI {
	
	public enum State {
		LOGIN, CONNECTED, MOVIE_LIST, REC_MOVIE, REC_USER, ERROR
	}
	
	private Group root;

	private Login login;

	private MovieList uiMovieList;

	private RecMovieList uiRecMovieList;

	private RecUserList uiRecUserList;

	private NextButton next;

	private ImageView background;

	private String message;

	State state = State.LOGIN;

	private String username;
	private Vector<Movie> movieList = new Vector<Movie>();
	private Vector<Movie> recMovie = new Vector<Movie>();
	private Vector<User> recUser = new Vector<User>();

	private int movieiter = 0;

	private Object listMutex = new Object();
	private Object loginMutex = new Object();
	private Object recMovieMutex = new Object();
	private Object recUserMutex = new Object();

	public GUI() {
		root = new Group();
		Image image = new Image(
				Login.class.getResourceAsStream("resources/Login.png"));
		background = new ImageView(image);

		login = new Login(this);

		root.getChildren().addAll(background, login);

		initError();
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public void setMessage(String message) {
		System.out.println(message);
		this.message = message;
	}

	public void changeState(State newState) {
		switch (newState) {
		case CONNECTED:
			showMainScene();
			break;
		case ERROR:
			showError();
			break;
		default:
			break;
		}
	}

	private final Rectangle upper = new Rectangle(800, 160,
			Color.LIGHTGOLDENRODYELLOW);
	private final Rectangle lower = new Rectangle(800, 160,
			Color.LIGHTGOLDENRODYELLOW);

	private final Rectangle center = new Rectangle(800, 160,
			Color.LIGHTGOLDENRODYELLOW);

	private final Group backGroup = new Group();

	private void showMainScene() {
		next = new NextButton();
		next.setOnMouseClicked(new NextEventHandler());
		uiMovieList = new MovieList(this);
		uiRecMovieList = new RecMovieList(this);
		uiRecUserList = new RecUserList(this);
		root.getChildren().remove(login);
		uiRecMovieList.relocate(0, 5);
		uiMovieList.relocate(0, 195);
		uiRecUserList.relocate(0, 400);

		upper.relocate(0, 0);
		center.relocate(0, 220);
		lower.relocate(0, 440);
		upper.setOpacity(0.0);
		center.setOpacity(0.0);
		lower.setOpacity(0.0);
		upper.setOnMouseEntered(new BackEventHandler(0));
		center.setOnMouseEntered(new BackEventHandler(1));
		lower.setOnMouseEntered(new BackEventHandler(2));
		backGroup.getChildren().addAll(upper, center, lower);

		root.getChildren().addAll(uiRecMovieList, uiMovieList, uiRecUserList,
				next, backGroup);
		center.setVisible(false);
		this.setState(State.MOVIE_LIST);
	}

	private final Group errGroup = new Group();
	private final Rectangle errorPane = new Rectangle(800, 600);
	private final Text errText = new Text();

	private void initError() {
		errorPane.setFill(Color.DARKGRAY);
		errorPane.setOpacity(0.5);
		errText.setFont(new Font(30));
		errText.setWrappingWidth(300);
		errText.setFill(Color.RED);
		errText.relocate(120, 100);
		errGroup.getChildren().addAll(errorPane, errText);
	}

	private void showError() {
		errText.setText(message);
		root.getChildren().add(errGroup);
		errGroup.setOnMouseClicked(new EventHandler<MouseEvent>() {
			@Override
			public void handle(MouseEvent event) {
				root.getChildren().remove(errGroup);
			}

		});
	}

	private class NextEventHandler implements EventHandler<MouseEvent> {

		@Override
		public void handle(MouseEvent event) {
			switch (next.getCurrentPos()) {
			case 0:
				uiRecMovieList.next();
				break;
			case 1:
				uiMovieList.next();
				break;
			case 2:
				uiRecUserList.next();
				break;
			default:
				break;
			}
		}

	}

	private class BackEventHandler implements EventHandler<MouseEvent> {

		private int index;

		public BackEventHandler(int index) {
			this.index = index;
		}

		@Override
		public void handle(MouseEvent event) {
			backGroup.getChildren().get(index).setVisible(false);
			backGroup.getChildren().get((index + 1) % 3).setVisible(true);
			backGroup.getChildren().get((index + 2) % 3).setVisible(true);

			ObservableList<Node> children = root.getChildren();
			children.get(1 + index).setScaleX(1.0);
			children.get(1 + index).setScaleY(1.0);
			children.get(1 + (index + 1) % 3).setScaleX(0.8);
			children.get(1 + (index + 1) % 3).setScaleY(0.8);
			children.get(1 + (index + 2) % 3).setScaleX(0.8);
			children.get(1 + (index + 2) % 3).setScaleY(0.8);
			next.gotoNextTransition(index);
		}

	}

	private void reLayout(double x, double y) {

		background.relocate(x - 800, y - 600);
		if (state.equals(State.LOGIN))
			return;

		double pos = y / 3;

		double nx = x - 50;
		double ny = next.getCurrentPos() * pos + 50;

		next.set(nx, ny, pos);

		uiRecMovieList.relocate(0, 5);
		uiMovieList.relocate(0, pos + 15);
		uiRecUserList.relocate(0, 2 * pos + 30);

		upper.relocate(0, 0);
		center.relocate(0, pos + 20);
		lower.relocate(0, 2 * pos + 30);

	}

	private Scene scene;

	private class ZoomHandler implements EventHandler<Event> {

		private double x = 800;
		private double y = 600;

		@Override
		public void handle(Event event) {
			// System.out.println("zoom");
			if (scene.getWidth() == x && scene.getHeight() == y)
				return;

			x = scene.getWidth();
			y = scene.getHeight();
			reLayout(x, y);
		}
	}

	@Override
	public void start(Stage primaryStage) throws Exception {
		scene = new Scene(root, 800, 600);
		scene.setFill(Color.LIGHTSTEELBLUE);

		primaryStage.addEventHandler(javafx.event.EventType.ROOT,
				new ZoomHandler());

		// primaryStage.setResizable(false);
		primaryStage.setMinHeight(600);
		primaryStage.setMinWidth(600);
		primaryStage.setTitle("I am GUI");
		primaryStage.setScene(scene);
		primaryStage.centerOnScreen();
		primaryStage.show();
	}

	@Override
	public void stop() throws Exception {
		cmdClose();
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public Vector<Movie> getRecMovie() {
		return recMovie;
	}

	public Vector<User> getRecUser() {
		return recUser;
	}

	public Object getLoginMutex() {
		return loginMutex;
	}

	private class MovieCacher implements Runnable {
		@Override
		public void run() {
			while (true) {
				int size;
				synchronized (listMutex) {
					size = movieList.size();
					if (size - movieiter < 10) {
						System.out.println(size + "!!!!!!!!!!!!!!!!!!!!!!"
								+ movieiter);
						cmdList();
					}
					while (size - movieiter < 10)
						try {
							listMutex.wait();
							size = movieList.size();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
				}

				int i = movieiter;
				while (i < size) {
					Movie m = movieList.get(i++);
					if (m.isInited())
						continue;

					MovieDetailDetector.getMovieDetails(m);
					MovieDetailDetector.getProperties(m);
					synchronized (listMutex) {
						size = movieList.size();
						listMutex.notifyAll();
					}
				}
				try {
					Thread.sleep(300);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public Movie nextMovie() {
		System.out.println("next movie");
		synchronized (listMutex) {
			while (movieiter >= movieList.size()) {
				try {
					listMutex.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		Movie ret = null;
		synchronized (listMutex) {
			if (movieiter < movieList.size())
				ret = movieList.get(movieiter++);
		}
		return ret;
	}

	public void cmdConnect(String[] args) {
		boolean useParam = false;
		if (args != null && args.length == 2)
			useParam = true;
		notifyListener(this, useParam, EventType.CONNECT, args);
	}

	public void cmdList() {
		notifyListener(this, false, EventType.LIST_MOVIE, null);
	}

	public void cmdRating(Movie movie, int rating) {
		notifyRatingListener(this, movie, rating);
	}

	public void cmdClose() {
		notifyListener(this, false, EventType.COLSE, null);
	}

	@Override
	public void printMovieList(List<Movie> list) {
		synchronized (listMutex) {
			movieList.addAll(list);
			System.out.println("print movie list : " + movieList.size() + "  "
					+ movieiter + " " + list.size());
			listMutex.notifyAll();
		}
	}

	@Override
	public void printRecMovie(List<Movie> list) {
		synchronized (recMovieMutex) {
			this.recMovie.clear();
			this.recMovie.addAll(list);
			Platform.runLater(uiRecMovieList.getRecMovieListUpdater());
		}
	}

	@Override
	public void printRecUser(List<User> list) {
		synchronized (recUserMutex) {
			this.recUser.clear();
			this.recUser.addAll(list);
			Platform.runLater(uiRecUserList.getRecUserListUpdater());
		}
	}

	@Override
	public void printErrMessage(String errMess) {
		synchronized (loginMutex) {
			setMessage(errMess);
			setState(State.ERROR);
			loginMutex.notify();
		}
	}

	@Override
	public void printConnectMessage(String message) {
		this.cmdList();
		new Thread(new MovieCacher()).start();
		synchronized (loginMutex) {
			setMessage(message);
			setState(State.CONNECTED);
			loginMutex.notify();
		}
	}

	@Override
	public void printCloseMessage(String message) {
		setMessage(message);
		changeState(State.ERROR);
	}

	@Override
	public void clean() {
		try {
			super.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void clear() {
		// do nothing
	}

	public static class UIStarter implements UI.UIStarter {
		@Override
		public void show() {
			Application.launch(GUI.class, new String[0]);
		}
	}
}
