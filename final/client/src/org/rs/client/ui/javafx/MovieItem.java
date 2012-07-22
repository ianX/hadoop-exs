package org.rs.client.ui.javafx;

import javafx.event.EventHandler;
import javafx.geometry.Bounds;
import javafx.scene.Group;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.text.Font;
import javafx.scene.text.Text;

import org.rs.client.ui.www.MovieDetailDetector;
import org.rs.object.Movie;

public class MovieItem extends Group {
	private Movie movie;
	private ImageView moviePic;
	private Text details;
	private Text summaryShort;
	private Text summaryAll;
	private Text title;

	private static double sheight = 140;
	private static double mheight = 200;
	private static double lheight = 444;

	private double wh = 0.4;

	public MovieItem(Movie movie) {
		// TODO Auto-generated constructor stub
		super();
		this.movie = movie;
		MovieDetailDetector.getMovieDetails(movie);
		MovieDetailDetector.getProperties(movie);
		moviePic = new ImageView(movie.getImageURL());
		Bounds bd = moviePic.getLayoutBounds();
		wh = bd.getWidth() / bd.getHeight();
		// moviePic.setScaleX(sheight / bd.getHeight());
		// moviePic.setScaleY(sheight / bd.getHeight());
		moviePic.setFitHeight(sheight);
		moviePic.setFitWidth(sheight * wh);
		moviePic.setCache(true);
		// moviePic.resize(sheight * wh, sheight);
		// moviePic.relocate(10, 0);
		title = new Text(movie.getName());
		title.setFont(new Font(30));
		title.relocate(120, 0);
		details = new Text();
		details.maxWidth(100);
		String s = "";
		for (String p : movie.getProperties()) {
			s += p + "\n";
		}
		details.setText(s);
		details.relocate(120, 40);
		title.setVisible(false);
		details.setVisible(false);
		this.getChildren().addAll(moviePic, title, details);
		this.setStyle("-fx-background-color: black;");
		this.setOnMouseClicked(new EventHandler<MouseEvent>() {
			@Override
			public void handle(MouseEvent event) {
				// TODO Auto-generated method stub
				MovieItem movieItem = (MovieItem)event.getSource();
				movieItem.title.setVisible(true);
				movieItem.details.setVisible(true);
			}
		});
	}
}
