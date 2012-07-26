package org.rs.client.ui.javafx;

import org.rs.client.ui.GUI;
import org.rs.object.User;

import javafx.animation.Transition;
import javafx.event.EventHandler;
import javafx.scene.Group;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.util.Duration;

public class UserItem extends Group {

	private class UserTransition extends Transition {

		private double from;
		private double len;

		private boolean op;

		public UserTransition(int state) {
			setCycleDuration(Duration.millis(200));
			switch (state) {
			case 0:
				op = true;
				from = 80;
				len = 43;
				break;
			case 1:
				op = false;
				from = 123;
				len = -43;
				break;
			default:
				break;
			}
		}

		@Override
		protected void interpolate(double frac) {
			if (op)
				info.setOpacity(frac);
			else
				info.setOpacity(1 - frac);
			info.relocate(0, from + len * frac);
		}
	}

	private final ImageView avatar = new ImageView();
	private final Text info = new Text();

	public UserItem(final User user, final GUI gui) {
		Image image = new Image(
				UserItem.class.getResourceAsStream("resources/avatar.png"));
		avatar.setImage(image);
		info.setText("name: " + user.getName() + "\nUID: " + user.getUid());
		avatar.setFitHeight(116);
		avatar.setFitWidth(116);
		info.relocate(0, 80);
		info.setOpacity(0);
		info.setFont(new Font(18));

		avatar.setOnMouseEntered(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				new UserTransition(0).play();
			}

		});

		avatar.setOnMouseExited(new EventHandler<MouseEvent>() {

			@Override
			public void handle(MouseEvent event) {
				new UserTransition(1).play();
			}

		});

		this.getChildren().addAll(avatar, info);
	}
}
