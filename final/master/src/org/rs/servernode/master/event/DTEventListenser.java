package org.rs.servernode.master.event;

import java.util.EventListener;

public interface DTEventListenser extends EventListener, Runnable {

	public void handleRatingEvent(RatingEvent event);

	public void handleListMovieEvent(ListMovieEvent event);

	public void handleRecMovieEvent(RecMovieEvent event);

	public void handleRecUserEvent(RecUserEvent event);
}
