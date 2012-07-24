package org.rs.client.ui.javafx;

import java.util.List;

import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.effect.DropShadow;
import javafx.scene.layout.HBox;

import org.rs.client.ui.GUI;
import org.rs.object.User;

public class RecUserList extends Parent {
	private class RecUserListUpdater implements Runnable {
		@Override
		public void run() {
			System.out.println("RecMovieListUpdater running");
			List<User> recUser = gui.getRecUser();
			int oldLen = hbox.getChildren().size();
			for (User u : recUser) {
				System.out.println(u.toString());
				hbox.getChildren().add(new UserItem(u, gui));
			}
			for (int i = 0; i < oldLen; i++) {
				hbox.getChildren().remove(0);
			}
			System.out.println("RecMovieListUpdater end");
		}
	}

	public RecUserListUpdater getRecUserListUpdater() {
		return new RecUserListUpdater();
	}

	private GUI gui;

	private static final int SPACE = 10;
	private static final int HEIGHT = 400;

	private HBox hbox = new HBox(SPACE);

	public RecUserList(GUI gui) {
		this.gui = gui;

		DropShadow shadow = new DropShadow();

		this.setEffect(shadow);

		hbox.setAlignment(Pos.CENTER);
		hbox.setPrefHeight(HEIGHT);

		this.getChildren().addAll(hbox);
	}
}
