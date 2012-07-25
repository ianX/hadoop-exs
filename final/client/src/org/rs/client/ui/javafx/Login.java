package org.rs.client.ui.javafx;

import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.Text;

import org.rs.client.ui.GUI;

public class Login extends Parent {
	public Login(final GUI gui) {
		final TextField name = new TextField();
		name.setPromptText("your name");
		final TextField host = new TextField();
		host.setPromptText("server host");
		final TextField port = new TextField();
		port.setPromptText("server port");
		final Button button = new Button("Login");
		button.setFont(new Font(14));
		button.setTextFill(Color.GREEN);

		final Text errmess = new Text();

		VBox vbox = new VBox(10);

		vbox.setAlignment(Pos.CENTER);
		vbox.setPrefSize(250, 250);
		vbox.getChildren().addAll(name, host, port, button, errmess);

		this.relocate(100, 200);
		this.getChildren().add(vbox);

		errmess.setVisible(false);

		button.setOnAction(new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent event) {
				if (name.getText().length() != 0
						&& host.getText().length() != 0
						&& port.getText().length() != 0) {
					String[] args = new String[2];
					args[0] = host.getText();
					args[1] = port.getText();
					gui.cmdConnect(args);
					gui.setUsername(name.getText());

					Object mutex = gui.getLoginMutex();
					boolean error = false;
					synchronized (mutex) {
						while (!gui.getState()
								.equals(GUI.State.CONNECTED)) {
							try {
								System.out.println("start waiting");
								mutex.wait();
								System.out.println("start changing");
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							if (gui.getState()
									.equals(GUI.State.ERROR)) {
								error = true;
								break;
							}
						}
					}
					if (error) {
						gui.changeState(GUI.State.ERROR);
						return;
					}
					gui.changeState(GUI.State.CONNECTED);
					System.out.println("state changed");
				} else {
					errmess.setText("(*^__^*)");
					errmess.setVisible(true);
				}
			}
		});
	}
}
