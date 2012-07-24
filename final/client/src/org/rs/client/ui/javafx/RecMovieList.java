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
			System.out.println("RecMovieListUpdater running");
			List<Movie> recMovie = gui.getRecMovie();
			int oldLen = hbox.getChildren().size();
			for (Movie m : recMovie) {
				System.out.println(m.toString());
				hbox.getChildren().add(new MovieItem(m, gui));
			}
			for (int i = 0; i < oldLen; i++) {
				hbox.getChildren().remove(0);
			}
			RecMovieList.this.setNeedsLayout(true);
			System.out.println("RecMovieListUpdater end");
		}
	}

	public RecMovieListUpdater getRecMovieListUpdater() {
		return new RecMovieListUpdater();
	}

	private GUI gui;

	private static final int SPACE = 10;
	private static final int HEIGHT = 400;

	private HBox hbox = new HBox(SPACE);

	public RecMovieList(GUI gui) {
		this.gui = gui;

		DropShadow shadow = new DropShadow();

		this.setEffect(shadow);

		hbox.setAlignment(Pos.CENTER);
		hbox.setPrefHeight(HEIGHT);
		this.getChildren().addAll(hbox);
	}
}
