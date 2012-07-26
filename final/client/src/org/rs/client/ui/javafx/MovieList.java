package org.rs.client.ui.javafx;

import java.util.Iterator;

import javafx.application.Platform;
import javafx.collections.ListChangeListener;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.effect.DropShadow;
import javafx.scene.layout.HBox;

import org.rs.client.ui.GUI;
import org.rs.object.Movie;

public class MovieList extends Parent implements ListChangeListener<Node> {
	private GUI gui;

	private static final int SHOW_SIZE = 10;
	private static final int SPACE = 10;
	private static final int MIN = 5;

	private HBox hbox = new HBox(SPACE);

	public MovieList(final GUI gui) {
		this.gui = gui;
		System.out.println("movielist start");

		DropShadow shadow = new DropShadow();

		this.setEffect(shadow);

		System.out.println("movielist end");
		hbox.getChildren().addListener(this);
		hbox.setAlignment(Pos.TOP_LEFT);

		this.getChildren().addAll(hbox);
		Platform.runLater(new Runnable() {
			@Override
			public void run() {
				Movie movie;
				while ((movie = gui.nextMovie()) != null
						&& hbox.getChildren().size() < SHOW_SIZE) {
					hbox.getChildren().add(new MovieItem(movie, gui));
				}
			}
		});
	}

	@Override
	public void onChanged(
			javafx.collections.ListChangeListener.Change<? extends Node> c) {
		while (c.next()) {
			if (c.wasAdded())
				return;
		}
		synchronized (this) {
			Movie movie;
			while (hbox.getChildren().size() < SHOW_SIZE
					&& (movie = gui.nextMovie()) != null) {
				System.out.println("add movie:" + movie.toString());
				hbox.getChildren().add(new MovieItem(movie, gui));
			}
		}
	}

	public void next() {
		if (hbox.getChildren().size() > MIN) {
			Iterator<Node> iter = hbox.getChildren().iterator();
			iter.next();
			iter.remove();
		}
	}
}
