package org.rs.client.ui;

import java.util.List;
import java.util.Vector;

import javafx.animation.Animation;
import javafx.animation.StrokeTransition;
import javafx.animation.Transition;
import javafx.application.Application;
import javafx.event.EventHandler;
import javafx.geometry.Pos;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.effect.Shadow;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.Pane;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.shape.CubicCurve;
import javafx.scene.shape.HLineTo;
import javafx.scene.shape.MoveTo;
import javafx.scene.shape.Path;
import javafx.scene.shape.Polygon;
import javafx.scene.shape.QuadCurve;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import javafx.util.Duration;

import org.rs.client.event.EventType;
import org.rs.client.ui.javafx.MovieItem;
import org.rs.object.Movie;
import org.rs.object.User;

public class GUI extends UI {

	private Vector<Movie> movieList = new Vector<Movie>();
	private Vector<Movie> recMovie = new Vector<Movie>();
	private Vector<User> recUser = new Vector<User>();
	private String[] connection = new String[] { "localhost", "6000" };

	public GUI() {
		// TODO Auto-generated constructor stub
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
		// TODO Auto-generated method stub
		// mainWindow.printMovieList(list);
	}

	@Override
	public void printRecMovie(List<Movie> list) {
		// TODO Auto-generated method stub
		// mainWindow.printRecMovie(list);
	}

	@Override
	public void printRecUser(List<User> list) {
		// TODO Auto-generated method stub
		// mainWindow.printRecUser(list);
	}

	@Override
	public void printErrMessage(String errMess) {
		// TODO Auto-generated method stub
		// mainWindow.printErrMessage(errMess);
	}

	@Override
	public void printConnectMessage(String message) {
		// TODO Auto-generated method stub
		// mainWindow.printConnectMessage(message);
	}

	@Override
	public void printCloseMessage(String message) {
		// TODO Auto-generated method stub
		// mainWindow.printCloseMessage(message);
	}

	@Override
	public void clean() {
		// TODO Auto-generated method stub
		// mainWindow.clean();
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		// mainWindow.clear();
	}

	@Override
	public void start(Stage primaryStage) throws Exception {
		// TODO Auto-generated method stub
		Group g = new Group();

		CubicCurve cubic = new CubicCurve();
		cubic.setStartX(0.0f);
		cubic.setStartY(50.0f);
		cubic.setControlX1(25.0f);
		cubic.setControlY1(0.0f);
		cubic.setControlX2(75.0f);
		cubic.setControlY2(100.0f);
		cubic.setEndX(100.0f);
		cubic.setEndY(50.0f);

		Path path = new Path();
		path.getElements().add(new MoveTo(0.0f, 0.0f));
		path.getElements().add(new HLineTo(80.0f));

		Polygon polygon = new Polygon();
		polygon.getPoints().addAll(
				new Double[] { 0.0, 0.0, 30.0, 10.0, 50.0, 0.0, 40.0, 20.0,
						60.0, 50.0, 30.0, 60.0, 20.0, 40.0, 0.0, 50.0, 20.0,
						20.0 });

		QuadCurve quad = new QuadCurve();
		quad.setStartX(0.0f);
		quad.setStartY(50.0f);
		quad.setEndX(50.0f);
		quad.setEndY(50.0f);
		quad.setControlX(25.0f);
		quad.setControlY(0.0f);

		final Rectangle rect = new Rectangle();
		rect.setHeight(100);
		rect.setWidth(100);
		rect.setArcHeight(50);
		rect.setArcWidth(50);
		rect.setFill(null);

		Rectangle r2 = new Rectangle();
		r2.setHeight(50);
		r2.setWidth(90);
		// r2.setFill(null);

		StrokeTransition st = new StrokeTransition(Duration.millis(3000), rect,
				Color.RED, Color.BLUE);
		st.setCycleCount(4);
		st.setAutoReverse(true);

		st.play();

		final String content = "Lorem ipsum";
		final Text text = new Text(10, 20, "");

		Image image = new Image("http://img1.douban.com/mpic/s9127643.jpg");
		ImageView imageView = new ImageView(image);
		imageView.setScaleX(0.5);
		imageView.setScaleY(0.5);
		imageView.setOnMouseClicked(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				// TODO Auto-generated method stub
				double x = event.getX();
				double y = event.getY();
				ImageView imageView = (ImageView) event.getSource();
				imageView.relocate(x, y);
			}
		});

		Text txt = new Text("Testing");
		txt.setFont(new Font(30));
		txt.relocate(180, 100);

		final Group canvas = new Group();
		canvas.setStyle("-fx-background-color: black;");
		Circle circle = new Circle(50, Color.BLUE);
		circle.relocate(200, 150);
		final Rectangle rectangle = new Rectangle(100, 100, Color.RED);
		rectangle.relocate(170, 70);
		canvas.getChildren().addAll(circle, rectangle, imageView, txt);

		final Animation animation = new Transition() {
			{
				cx = rectangle.getWidth();
				cy = rectangle.getHeight();
				setCycleDuration(Duration.millis(2000));
			}

			double cx;
			double cy;

			protected void interpolate(double frac) {
				rectangle.setHeight(cy + frac * 200);
			}

		};

		animation.setAutoReverse(true);
		animation.setCycleCount(Integer.MAX_VALUE);
		animation.play();

		MovieItem movieItem = new MovieItem(new Movie("kill bill 2"));

		StackPane stack = new StackPane();
		stack.getChildren().addAll(new Rectangle(100, 100, Color.BLUE),
				new Label("Go!"));

		Rectangle r3 = new Rectangle(400, 1000);
		r3.relocate(200, 0);

		VBox vbox = new VBox(20); // spacing
		vbox.setPrefWidth(800);
		vbox.setAlignment(Pos.CENTER);
		vbox.getChildren().addAll(new Button("Copy"), r2, canvas, movieItem,
				rect);

		Shadow shadow = new Shadow(10, Color.DARKGREEN);
		r3.setEffect(shadow);
		// vbox.setEffect(shadow);
		// vbox.setOpacity(0.5);
		g.getChildren().addAll(r3, vbox);
		Scene scene = new Scene(g, 800, 600);

		primaryStage.setMinWidth(600);
		primaryStage.setMinHeight(400);
		primaryStage.setTitle("I am GUI");
		primaryStage.setScene(scene);
		primaryStage.centerOnScreen();
		primaryStage.setOpacity(0.8);
		// primaryStage.setIconified(true);
		primaryStage.show();
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		super.stop();
		cmdClose();
	}

	public static class UIStarter implements UI.UIStarter {
		@Override
		public void show() {
			// TODO Auto-generated method stub
			Application.launch(GUI.class, new String[0]);
		}
	}
}
