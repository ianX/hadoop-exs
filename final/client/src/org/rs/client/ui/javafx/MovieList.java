package org.rs.client.ui.javafx;

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

	private static final int CACHE_SIZE = 3;
	private static final int SPACE = 10;
	private static final int HEIGHT = 400;

	private HBox hbox = new HBox(SPACE);

	public MovieList(GUI gui) {
		this.gui = gui;
		System.out.println("movielist start");

		DropShadow shadow = new DropShadow();

		this.setEffect(shadow);

		Movie movie;
		while ((movie = gui.nextMovie()) != null
				&& hbox.getChildren().size() < CACHE_SIZE) {
			hbox.getChildren().add(new MovieItem(movie, gui));
		}

		System.out.println("movielist end");
		hbox.getChildren().addListener(this);
		hbox.setAlignment(Pos.CENTER);
		hbox.setPrefHeight(HEIGHT);

		this.getChildren().addAll(hbox);
	}

	@Override
	public void onChanged(
			javafx.collections.ListChangeListener.Change<? extends Node> c) {
		// TODO Auto-generated method stub
		if (c.wasAdded())
			return;
		synchronized (this) {
			Movie movie;
			while (hbox.getChildren().size() < CACHE_SIZE
					&& (movie = gui.nextMovie()) != null) {
				System.out.println("add movie:" + movie.toString());
				hbox.getChildren().add(new MovieItem(movie, gui));
			}
		}
	}
}
