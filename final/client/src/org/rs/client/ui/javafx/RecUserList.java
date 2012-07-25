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
			System.out.println("RecUserListUpdater running");
			hbox.getChildren().clear();
			List<User> recUser = gui.getRecUser();
			for (User u : recUser) {
				System.out.println(u.toString());
				hbox.getChildren().add(new UserItem(u, gui));
			}
			System.out.println("RecUserListUpdater end");
		}
	}

	public RecUserListUpdater getRecUserListUpdater() {
		return new RecUserListUpdater();
	}

	private GUI gui;

	private static final int SPACE = 10;
	private static final int MIN = 5;

	private HBox hbox = new HBox(SPACE);

	public RecUserList(GUI gui) {
		this.gui = gui;

		DropShadow shadow = new DropShadow();

		this.setEffect(shadow);

		hbox.setAlignment(Pos.CENTER);

		this.getChildren().addAll(hbox);
	}

	public void next() {
		if (hbox.getChildren().size() > MIN) {
			hbox.getChildren().remove(0);
		}
	}
}
