package org.rs.client.ui;

import java.util.List;
import java.util.Vector;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.event.EventHandler;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.paint.Color;
import javafx.scene.shape.Polygon;
import javafx.stage.Stage;

import org.rs.client.event.EventType;
import org.rs.client.ui.javafx.Login;
import org.rs.client.ui.javafx.MovieList;
import org.rs.client.ui.javafx.RecMovieList;
import org.rs.client.ui.javafx.RecUserList;
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
		ImageView background = new ImageView();
		background.setImage(image);

		login = new Login(this);

		root.getChildren().addAll(background, login);
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
			connected();
			break;
		case MOVIE_LIST:
			break;
		case REC_MOVIE:
			break;
		case REC_USER:
			break;
		case ERROR:
			break;
		default:
			break;
		}
	}

	private final Group nextg = new Group();
	private final Polygon next = new Polygon();

	private void connected() {
		System.out.println("showing");
		uiMovieList = new MovieList(this);
		System.out.println("showing rec movie");
		uiRecMovieList = new RecMovieList(this);
		System.out.println("showing rec user");
		uiRecUserList = new RecUserList(this);
		root.getChildren().remove(login);
		uiRecMovieList.relocate(0, 0);
		uiMovieList.relocate(0, 100);
		uiRecUserList.relocate(0, 200);

		next.getPoints().addAll(
				new Double[] { 0.0, 0.0, 0.0, 100.0, 50.0, 50.0 });
		next.setFill(Color.LIGHTGREEN);
		next.setOpacity(0.3);
		nextg.getChildren().add(next);

		nextg.relocate(750, 150);

		nextg.setOnMouseEntered(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				next.setScaleX(1.1);
				next.setScaleY(1.1);
				next.setFill(Color.GREEN);
				next.setOpacity(0.5);
			}

		});

		nextg.setOnMouseExited(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				next.setScaleX(1.0);
				next.setScaleY(1.0);
				next.setFill(Color.LIGHTGREEN);
				next.setOpacity(0.3);
			}

		});

		System.out.println("showing main pane");
		root.getChildren().addAll(uiRecMovieList, uiRecUserList, uiMovieList,
				nextg);
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

	public Movie nextMovie() {
		synchronized (listMutex) {
			System.out.println("next movie");
			if (movieiter < movieList.size())
				return movieList.get(movieiter++);
			else {
				System.out.println("next movie : call cmdList()");
				this.cmdList();
				try {
					System.out.println("next movie: start waiting");
					listMutex.wait(1000);
					System.out.println("next movie: end waiting");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		if (movieiter < movieList.size())
			return movieList.get(movieiter++);
		else
			return null;
	}

	public Object getLoginMutex() {
		return loginMutex;
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
			System.out.println("print movie list");
			movieList.addAll(list);
			listMutex.notify();
		}
	}

	@Override
	public void printRecMovie(List<Movie> list) {
		synchronized (recMovieMutex) {
			this.recMovie.clear();
			this.recMovie.addAll(list);
			Platform.runLater(this.uiRecMovieList.getRecMovieListUpdater());
		}
	}

	@Override
	public void printRecUser(List<User> list) {
		synchronized (recUserMutex) {
			this.recUser.clear();
			this.recUser.addAll(list);
			Platform.runLater(this.uiRecUserList.getRecUserListUpdater());
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
		synchronized (loginMutex) {
			setMessage(message);
			setState(State.CONNECTED);
			loginMutex.notify();
		}
	}

	@Override
	public void printCloseMessage(String message) {
	}

	@Override
	public void clean() {
		try {
			super.stop();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void clear() {
	}

	@Override
	public void start(Stage primaryStage) throws Exception {
		Scene scene = new Scene(root, 800, 600);
		scene.setFill(Color.DARKSLATEGREY);
		primaryStage.setResizable(false);
		// primaryStage.setFullScreen(true);
		primaryStage.setTitle("I am GUI");
		primaryStage.setScene(scene);
		primaryStage.centerOnScreen();
		primaryStage.show();
	}

	@Override
	public void stop() throws Exception {
		cmdClose();
	}

	public static class UIStarter implements UI.UIStarter {
		@Override
		public void show() {
			Application.launch(GUI.class, new String[0]);
		}
	}
}
