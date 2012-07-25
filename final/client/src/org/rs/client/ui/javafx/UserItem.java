package org.rs.client.ui.javafx;

import org.rs.client.ui.GUI;
import org.rs.object.User;

import javafx.event.EventHandler;
import javafx.scene.Group;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.text.Font;
import javafx.scene.text.Text;

public class UserItem extends Group {
	private final ImageView avatar = new ImageView();
	private final Text info = new Text();

	public UserItem(final User user, final GUI gui) {
		Image image = new Image(
				UserItem.class.getResourceAsStream("resources/avatar.png"));
		avatar.setImage(image);
		info.setText("name: " + user.getName() + "\nUID: " + user.getUid());
		avatar.setFitHeight(120);
		avatar.setFitWidth(120);
		info.relocate(0, 135);
		info.setFont(new Font(20));
		
		this.setOnMouseEntered(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				avatar.setScaleX(1.1);
				avatar.setScaleY(1.1);
			}

		});

		this.setOnMouseExited(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				avatar.setScaleX(1.0);
				avatar.setScaleY(1.0);
			}

		});
		
		this.getChildren().addAll(avatar, info);
	}
}
