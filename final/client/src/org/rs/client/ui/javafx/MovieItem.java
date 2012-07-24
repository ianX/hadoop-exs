package org.rs.client.ui.javafx;

import javafx.application.Platform;
import javafx.event.EventHandler;
import javafx.geometry.Bounds;
import javafx.scene.Group;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Font;
import javafx.scene.text.Text;

import org.rs.client.ui.GUI;
import org.rs.client.ui.www.MovieDetailDetector;
import org.rs.object.Movie;

public class MovieItem extends Group {
	private GUI gui;

	private RatingPane ratingPane;
	private Movie movie;
	private final ImageView moviePic = new ImageView();
	private final Text details = new Text();
	private final Text title = new Text();

	private final Rectangle back = new Rectangle(200, 260, Color.LIGHTGRAY);

	private static final double mheight = 220;
	private double wh = 0.4;

	public MovieItem(final Movie movie, GUI gui) {
		super();

		this.gui = gui;
		this.movie = movie;

		System.out.println("new item");
		MovieDetailDetector.getMovieDetails(movie);
		Platform.runLater(new Runnable() {
			@Override
			public void run() {
				MovieDetailDetector.getProperties(movie);
				String s = "";
				for (String p : movie.getProperties()) {
					s += p + "\n";
				}
				details.setText(s);
			}
		});

		moviePic.setImage(new Image(movie.getImageURL()));

		// moviePic = new ImageView(
		// "file:///home/ian/Pictures/zaregoto_series/643376.jpg");
		Bounds bd = moviePic.getLayoutBounds();
		wh = bd.getWidth() / bd.getHeight();

		moviePic.setFitHeight(mheight);
		moviePic.setFitWidth(mheight * wh);
		moviePic.setCache(true);

		String name = movie.getName();
		title.setText(name);
		title.setFont(new Font(20));
		title.relocate(190, 0);
		details.relocate(190, title.layoutYProperty().get() + 10);
		title.setVisible(false);
		details.setVisible(false);
		title.setWrappingWidth(200);
		details.setWrappingWidth(200);

		ratingPane = new RatingPane(this);
		ratingPane.relocate(0, 245);
		ratingPane.setVisible(false);

		back.setOpacity(0.0);

		this.getChildren().addAll(back, moviePic, title, details, ratingPane);
		this.setStyle("-fx-background-color: black;");

		this.setOnMouseEntered(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				moviePic.setScaleX(1.1);
				moviePic.setScaleY(1.1);
				ratingPane.setVisible(true);
			}

		});

		this.setOnMouseExited(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				moviePic.setScaleX(1.0);
				moviePic.setScaleY(1.0);
				ratingPane.setVisible(false);
			}

		});

		moviePic.setOnMouseClicked(new EventHandler<MouseEvent>() {
			@Override
			public void handle(MouseEvent event) {
				title.setVisible(true);
				details.setVisible(true);
				ratingPane.setVisible(true);
			}
		});
	}

	public void urating(int rating) {
		System.out.println("rating: " + movie.toString() + " " + rating);
		gui.cmdRating(movie, rating);
	}
}
