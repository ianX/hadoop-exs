package org.rs.client.event;

import java.util.EventObject;

import org.rs.client.ui.UI;
import org.rs.object.Movie;

public class UIRatingEvent extends EventObject {

	private static final long serialVersionUID = -2261713589809363537L;
	private UI ui;
	private Movie movie;
	private int rating;

	public UIRatingEvent(UI source, Movie movie, int rating) {
		super(source);
		this.ui = source;
		this.movie = movie;
		this.rating = rating;
	}

	public UI getUi() {
		return ui;
	}

	public Movie getMovie() {
		return movie;
	}

	public int getRating() {
		return rating;
	}
}
