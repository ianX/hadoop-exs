package org.rs.client.ui.javafx;

import org.rs.client.ui.GUI;
import org.rs.object.User;

import javafx.scene.Group;

public class UserItem extends Group {
	private User user;
	private GUI gui;

	public UserItem(User user, GUI gui) {
		// TODO Auto-generated constructor stub
		this.user = user;
		this.gui = gui;
	}
}
