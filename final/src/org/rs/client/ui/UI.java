package org.rs.client.ui;

import java.util.List;
import java.util.Vector;

import org.rs.client.event.EventType;
import org.rs.client.event.UIEvent;
import org.rs.client.event.UIEventListener;
import org.rs.object.Movie;
import org.rs.object.User;

public abstract class UI implements Runnable {
	private EventType eventType = EventType.EMPTY;

	private Vector<UIEventListener> listeners = new Vector<UIEventListener>();

	private String[] params = null;

	public String[] getParams() {
		return params;
	}

	public void setParams(String[] params) {
		this.params = params;
	}

	public final void addUIEventListener(UIEventListener listener) {
		listeners.add(listener);
	}

	public final void removeUIEventListener(UIEventListener listener) {
		listeners.remove(listener);
	}

	public final void notifyListener(boolean useParams) {
		for (UIEventListener listener : listeners) {
			listener.handleUIEvent(new UIEvent(this, useParams));
		}
	}

	public final EventType getEventType() {
		return eventType;
	}

	public final void setEventType(EventType type) {
		eventType = type;
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
