package org.rs.client.ui.javafx;

import javafx.event.EventHandler;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;

public class RatingPane extends HBox {

	private class MouseClickedHandler implements EventHandler<MouseEvent> {

		private int index;

		public MouseClickedHandler(int index) {
			this.index = index;
		}

		@Override
		public void handle(MouseEvent event) {
			synchronized (RatingPane.this) {
				if (rated)
					return;
				rated = true;
			}
			circle[0].setFill(Color.DARKGRAY);
			for (int k = 1; k <= index; k++) {
				circle[k].setFill(Color.YELLOWGREEN);
			}
			item.urating(index);
		}
	}

	private boolean rated = false;

	private MovieItem item;

	private final Circle[] circle = new Circle[6];

	public RatingPane(MovieItem item) {
		this.item = item;
		
		this.setSpacing(8);

		for (int i = 0; i < 6; i++) {
			circle[i] = new Circle(7, Color.LIGHTGREEN);
			this.getChildren().add(circle[i]);
			final int j = i;

			circle[i].setOnMouseClicked(new MouseClickedHandler(i));

			circle[i].setOnMouseEntered(new EventHandler<MouseEvent>() {
				public void handle(MouseEvent event) {
					if (RatingPane.this.rated)
						return;
					circle[0].setFill(Color.DARKGRAY);
					for (int k = 1; k <= j; k++) {
						circle[k].setFill(Color.YELLOWGREEN);
					}
				}
			});
			circle[i].setOnMouseExited(new EventHandler<MouseEvent>() {
				public void handle(MouseEvent event) {
					if (RatingPane.this.rated)
						return;
					circle[j].setFill(Color.LIGHTGREEN);
				}
			});
		}
		this.setOnMouseExited(new EventHandler<MouseEvent>() {
			public void handle(MouseEvent event) {
				if (RatingPane.this.rated)
					return;
				for (int k = 0; k < 6; k++) {
					circle[k].setFill(Color.LIGHTGREEN);
				}
			}
		});
	}
}
