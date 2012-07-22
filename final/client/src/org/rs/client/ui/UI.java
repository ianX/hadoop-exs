package org.rs.client.ui;

import java.util.List;
import java.util.Vector;

import javafx.application.Application;

import org.rs.client.event.EventType;
import org.rs.client.event.UIEvent;
import org.rs.client.event.UIEventListener;
import org.rs.client.event.UIRatingEvent;
import org.rs.object.Movie;
import org.rs.object.User;

public abstract class UI extends Application {

	public static interface UIStarter{
		public void show();
	}
	
	private static final Vector<UIEventListener> listeners = new Vector<UIEventListener>();

	public static final void addUIEventListener(UIEventListener listener) {
		listeners.add(listener);
	}

	public static final void removeUIEventListener(UIEventListener listener) {
		listeners.remove(listener);
	}

	public static final void notifyRatingListener(UI ui, Movie movie, int rating) {
		for (UIEventListener listener : listeners) {
			listener.handleUIRatingEvent(new UIRatingEvent(ui, movie, rating));
		}
	}

	public static final void notifyListener(UI ui, boolean useParams,
			EventType eventType, String[] params) {
		for (UIEventListener listener : listeners) {
			listener.handleUIEvent(new UIEvent(ui, useParams, eventType, params));
		}
	}

	public abstract void printMovieList(List<Movie> list);

	public abstract void printRecMovie(List<Movie> list);

	public abstract void printRecUser(List<User> list);

	public abstract void printErrMessage(String errMess);

	public abstract void printConnectMessage(String message);

	public abstract void printCloseMessage(String message);

	public abstract void clear();

	public abstract void clean();
}
