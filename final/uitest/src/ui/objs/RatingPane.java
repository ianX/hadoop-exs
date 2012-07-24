package ui.objs;

import javafx.event.EventHandler;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;

public class RatingPane extends HBox {

	private boolean rated = false;

	private MovieItem item;

	public RatingPane(MovieItem item) {
		super(10);

		this.item = item;

		final Circle[] circle = new Circle[6];
		for (int i = 0; i < 6; i++) {
			circle[i] = new Circle(10, Color.LIGHTGRAY);
			this.getChildren().add(circle[i]);
			final int j = i;
			circle[i].setOnMouseClicked(new EventHandler<MouseEvent>() {
				public void handle(MouseEvent event) {
					if (RatingPane.this.rated)
						return;
					RatingPane.this.rated = true;
					circle[0].setFill(Color.GRAY);
					for (int k = 1; k <= j; k++) {
						circle[k].setFill(Color.YELLOWGREEN);
					}
					RatingPane.this.item.urating(j);
				}
			});
			circle[i].setOnMouseEntered(new EventHandler<MouseEvent>() {
				public void handle(MouseEvent event) {
					if (RatingPane.this.rated)
						return;
					circle[0].setFill(Color.GRAY);
					for (int k = 1; k <= j; k++) {
						circle[k].setFill(Color.YELLOWGREEN);
					}
				}
			});
			circle[i].setOnMouseExited(new EventHandler<MouseEvent>() {
				public void handle(MouseEvent event) {
					if (RatingPane.this.rated)
						return;
					circle[j].setFill(Color.LIGHTGRAY);
				}
			});
		}
		this.setOnMouseExited(new EventHandler<MouseEvent>() {
			public void handle(MouseEvent event) {
				if (RatingPane.this.rated)
					return;
				for (int k = 0; k < 6; k++) {
					circle[k].setFill(Color.LIGHTGRAY);
				}
			}
		});
	}
}
