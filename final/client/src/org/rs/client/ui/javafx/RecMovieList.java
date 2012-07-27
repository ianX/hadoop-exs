package org.rs.client.ui.javafx;

import java.util.List;

import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.effect.DropShadow;
import javafx.scene.layout.HBox;

import org.rs.client.ui.GUI;
import org.rs.object.Movie;

public class RecMovieList extends Parent {

	private class RecMovieListUpdater implements Runnable {
		@Override
		public void run() {
			// System.out.println("RecMovieListUpdater running");
			hbox.getChildren().clear();
			List<Movie> recMovie = gui.getRecMovie();
			// System.out.println("RecMovie " + recMovie.size());
			for (Movie m : recMovie) {
				// System.out.println(m.getName() + "to " + m.toString()
				// + " id : " + m.getMid());
				hbox.getChildren().add(new MovieItem(m, gui));
			}
			// System.out.println("RecMovieListUpdater end");
		}
	}

	public RecMovieListUpdater getRecMovieListUpdater() {
		return new RecMovieListUpdater();
	}

	private GUI gui;

	private static final int SPACE = 10;

	private static final int MIN = 5;

	private HBox hbox = new HBox(SPACE);

	public RecMovieList(GUI gui) {
		this.gui = gui;

		DropShadow shadow = new DropShadow();

		this.setEffect(shadow);

		hbox.setAlignment(Pos.TOP_LEFT);
		this.getChildren().addAll(hbox);
	}

	public void next() {
		if (hbox.getChildren().size() > MIN) {
			hbox.getChildren().remove(0);
		}
	}
}
