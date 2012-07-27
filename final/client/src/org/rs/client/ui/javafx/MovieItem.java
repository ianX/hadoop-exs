package org.rs.client.ui.javafx;

import javafx.animation.Transition;
import javafx.event.EventHandler;
import javafx.scene.Group;
import javafx.scene.control.Label;
import javafx.scene.effect.Light;
import javafx.scene.effect.Lighting;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Font;
import javafx.util.Duration;

import org.rs.client.ui.GUI;
import org.rs.client.ui.www.MovieDetailDetector;
import org.rs.object.Movie;

public class MovieItem extends Group {

	private class DetailsTransition extends Transition {

		private boolean type;

		public DetailsTransition(boolean type) {
			setCycleDuration(Duration.millis(200));
			this.type = type;
		}

		@Override
		protected void interpolate(double frac) {
			if (type) {
				title.setScaleX(frac);
				title.setOpacity(frac);
				details.setScaleX(frac);
				details.setOpacity(frac);
				title.relocate(160 * frac, 0);
				details.relocate(160 * frac, 40);
			} else {
				title.setScaleX(1 - frac);
				title.setOpacity(1 - frac);
				details.setScaleX(1 - frac);
				details.setOpacity(1 - frac);
				title.relocate(160 - 160 * frac, 0);
				details.relocate(160 - 160 * frac, 40);
			}
			// detailBack.resize(fromX + lenX * frac, fromY + lenY * frac);
		}

	}

	private class RatingTransition extends Transition {

		private double from;
		private double len;

		private boolean op;

		public RatingTransition(int state) {
			setCycleDuration(Duration.millis(200));
			switch (state) {
			case 0:
				op = true;
				from = 160;
				len = 28;
				break;
			case 1:
				op = false;
				from = 188;
				len = -28;
				break;
			default:
				break;
			}
		}

		@Override
		protected void interpolate(double frac) {
			if (op)
				ratingPane.setOpacity(frac);
			else
				ratingPane.setOpacity(1 - frac);
			ratingPane.relocate(5, from + len * frac);
		}
	}

	private GUI gui;

	private RatingPane ratingPane;
	private Movie movie;
	private ImageView moviePic = new ImageView();
	private Label details;
	private Label title;

	final Rectangle back = new Rectangle(170, 220, Color.LIGHTGRAY);

	private static final double height = 178;
	private double wh = 0.71;

	// private Rectangle detailBack = new Rectangle(126, 178, Color.DARKGRAY);

	public void getDetails() {
		if (movie.getProperties() == null)
			MovieDetailDetector.getProperties(movie);

		int line = 0;
		String s = "";
		for (String p : movie.getProperties()) {
			s += p + "\n";
			line++;
			if (s.length() >= 200 || line >= 10) {
				s += "……";
				break;
			}
		}
		if (s.length() == 0) {
			s = "导演:未知\n主演:未知\n类型:未知\n地区:未知\n上映时间:未知";
			/*
			 * line = 0; for (int i = 0; i < 12; i++) { line++; s += i +
			 * "xxxxxxxxxxxxxxxxxxx\n"; if (line >= 10) { s += "……"; break; } }
			 */
		}
		details = new Label(s);
		details.setWrapText(true);
		details.setMaxSize(250, 160);
		details.relocate(0, title.getMaxHeight() + 3);
		details.setScaleX(0);
		details.setOpacity(0);
		MovieItem.this.getChildren().add(details);
	}

	private boolean isExpanded = false;
	private boolean detailed = false;

	private Light.Distant light = new Light.Distant();
	private Lighting lighting = new Lighting();

	public MovieItem(final Movie movie, GUI gui) {

		lighting.setLight(light);
		lighting.setSurfaceScale(1.0);

		this.gui = gui;
		this.movie = movie;

		//System.out.println("new item");

		if (!movie.isInited())
			MovieDetailDetector.getMovieDetails(movie);

		String imageURL = movie.getImageURL();

		if (imageURL.length() == 0) {
			moviePic.setImage(new Image(MovieItem.class
					.getResourceAsStream("resources/default_movie_pic.jpg")));
		} else {
			moviePic.setImage(new Image(imageURL, true));
		}

		// Bounds bd = moviePic.getLayoutBounds();
		// wh = bd.getWidth() / bd.getHeight();

		moviePic.setFitHeight(height);
		moviePic.setFitWidth(height * wh);
		// moviePic.setCache(true);

		String name = movie.getName();
		title = new Label(name);
		title.setFont(new Font(18));
		// title.setVisible(false);

		title.setWrapText(true);
		title.setMaxSize(250, 36);

		title.setScaleX(0);
		title.setOpacity(0);
		// title.relocate(160, 0);

		title.setEffect(lighting);

		title.setTextFill(Color.DARKGOLDENROD);

		ratingPane = new RatingPane(this);
		ratingPane.setOpacity(0.0);
		ratingPane.relocate(5, 160);

		back.setOpacity(0.0);

		this.getChildren().addAll(back, moviePic, title, ratingPane);
		this.setStyle("-fx-background-color: black;");

		moviePic.setOnMouseEntered(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				new RatingTransition(0).play();
				if (isExpanded)
					return;
				synchronized (this) {
					isExpanded = true;
					// title.setVisible(true);
					if (!detailed) {
						getDetails();
						detailed = true;
					}
					new DetailsTransition(true).play();
				}
			}

		});

		this.setOnMouseExited(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				if (isExpanded) {
					synchronized (this) {
						isExpanded = false;
						new DetailsTransition(false).play();
					}
				}
				new RatingTransition(1).play();
			}
		});
	}

	public void urating(int rating) {
		System.out.println("rating: " + movie.getMid() + "," + movie.toString()
				+ " " + rating);
		gui.cmdRating(movie, rating);
	}
}
