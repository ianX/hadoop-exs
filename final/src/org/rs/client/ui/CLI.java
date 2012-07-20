package org.rs.client.ui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.rs.client.event.EventType;
import org.rs.object.Movie;
import org.rs.object.User;

public class CLI extends UI {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		BufferedReader cmdReader = new BufferedReader(new InputStreamReader(
				System.in));
		String cmd;
		try {
			while ((cmd = cmdReader.readLine()) != null) {
				cmdHandler(cmd.trim());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("cmdreader err, quiting");
			this.setEventType(EventType.COLSE);
			this.notifyListener(false);
		}
	}

	private void cmdHandler(String cmd) {
		if (cmd.startsWith("close")) {
			this.setEventType(EventType.COLSE);
			this.notifyListener(false);
		} else if (cmd.startsWith("connect")) {
			String[] args = cmd.split(" ");
			boolean useParam = true;
			if (args.length == 1) {
				useParam = false;
			} else if (args.length == 3) {
				String[] param = new String[2];
				param[0] = args[1];
				param[1] = args[2];
				this.setParams(param);
			} else {
				this.printErrMessage("cmd format error (usage: connect host port)");
				return;
			}
			this.setEventType(EventType.CONNECT);
			this.notifyListener(useParam);
		} else if (cmd.startsWith("list")) {
			this.setEventType(EventType.LIST_MOVIE);
			this.notifyListener(false);
		} else if (cmd.startsWith("rate")) {
			int i = cmd.indexOf(' ');
			int j = cmd.lastIndexOf(' ');
			if (i >= j) {
				this.printErrMessage("cmd format error (usage: rate movie rating)");
				return;
			}
			String[] param = new String[2];
			param[0] = cmd.substring(i + 1, j);
			param[1] = cmd.substring(j + 1);
			this.setParams(param);
			this.setEventType(EventType.RATING);
			this.notifyListener(true);
		} else if (cmd.length() == 0) {
			return;
		} else {
			this.printErrMessage("Unrecognized cmd : " + cmd);
		}
	}

	@Override
	public void printMovieList(List<Movie> list) {
		// TODO Auto-generated method stub
		System.out.println("movie list:");
		for (Movie movie : list) {
			System.out.println(movie.toString());
		}
	}

	@Override
	public void printRecMovie(List<Movie> list) {
		// TODO Auto-generated method stub
		System.out.println("rec movies:");
		for (Movie movie : list) {
			System.out.println(movie.toString());
		}
	}

	@Override
	public void printRecUser(List<User> list) {
		// TODO Auto-generated method stub
		System.out.println("rec users:");
		for (User user : list) {
			System.out.println(user.toString());
		}
	}

	@Override
	public void printErrMessage(String errMess) {
		// TODO Auto-generated method stub
		System.out.println(errMess);
	}

	@Override
	public void printConnectMessage(String message) {
		// TODO Auto-generated method stub
		System.out.println(message);
	}

	@Override
	public void printCloseMessage(String message) {
		// TODO Auto-generated method stub
		System.out.println(message);
	}

	@Override
	public void clean() {
		// TODO Auto-generated method stub
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		try {
			Runtime.getRuntime().exec("clear");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
