package org.rs.client.ui.javafx;

import javafx.animation.Transition;
import javafx.event.EventHandler;
import javafx.scene.Group;
import javafx.scene.effect.MotionBlur;
import javafx.scene.input.MouseEvent;
import javafx.scene.paint.Color;
import javafx.scene.shape.Polygon;
import javafx.util.Duration;

public class NextButton extends Group {

	private class NextTransition extends Transition {

		private double move;

		public NextTransition(int newPos) {
			move = (newPos - currentPos) * moveLen;
			currentPos = newPos;
			oldY = layoutY;
			layoutY = oldY + move;
			this.setCycleDuration(Duration.millis(300));
		}

		@Override
		protected void interpolate(double frac) {
			NextButton.this.relocate(layoutX, oldY + move * frac);
		}
	}

	public void gotoNextTransition(int newPos) {
		new NextTransition(newPos).play();
	}

	private double moveLen = 200;
	private double layoutX = 750;
	private double layoutY = 210;

	private double oldY = 200;

	private int currentPos = 1;

	public void set(double x, double y, double moveLen) {
		this.layoutX = x;
		this.layoutY = y;
		this.moveLen = moveLen;
		this.relocate(x, y);
	}

	private final Polygon next = new Polygon();

	public int getCurrentPos() {
		return currentPos;
	}

	public NextButton() {

		MotionBlur motionBlur = new MotionBlur();
		motionBlur.setRadius(3);
		motionBlur.setAngle(-15.0);

		next.setEffect(motionBlur);

		next.getPoints().addAll(
				new Double[] { 0.0, 0.0, 0.0, 100.0, 50.0, 50.0 });
		next.setFill(Color.GREEN);
		next.setOpacity(0.5);
		this.getChildren().add(next);

		this.relocate(layoutX, layoutY);

		this.setOnMouseEntered(new EventHandler<MouseEvent>() {
			@Override
			public void handle(MouseEvent event) {
				next.setScaleX(1.1);
				next.setScaleY(1.1);
				next.setFill(Color.DARKGREEN);
				next.setOpacity(0.7);
			}

		});

		this.setOnMouseExited(new EventHandler<MouseEvent>() {
			@Override
			public void handle(MouseEvent event) {
				next.setScaleX(1.0);
				next.setScaleY(1.0);
				next.setFill(Color.GREEN);
				next.setOpacity(0.3);
			}

		});
	}

}
