package ui;

import org.rs.object.Movie;

import ui.objs.MovieItem;
import javafx.application.Application;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.layout.HBox;
import javafx.stage.Stage;

public class Main extends Application {

	@Override
	public void start(Stage primaryStage) throws Exception {
		// TODO Auto-generated method stub
		HBox box = new HBox(10);
		box.setAlignment(Pos.CENTER);
		box.setPrefHeight(400);

		MovieItem movieItem = new MovieItem(new Movie("kill bill"));

		box.getChildren().add(movieItem);

		Scene scene = new Scene(box, 800, 600);

		primaryStage.setMinWidth(600);
		primaryStage.setMinHeight(400);
		primaryStage.setTitle("I am GUI");
		primaryStage.setScene(scene);
		primaryStage.centerOnScreen();
		// primaryStage.setOpacity(0.8);
		// primaryStage.setIconified(true);
		primaryStage.show();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Application.launch();
	}

}
