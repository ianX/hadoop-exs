package org.rs.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.rs.server.io.DataGetter;
import org.rs.server.io.MasterDataGetter;
import org.rs.servernode.master.ServerMaster;
import org.rs.servernode.master.event.DTEventListenser;

public class Server {

	private ServerSocket serverSocket;
	private DataGetter db;
	private int port;
	private int maxLinker = 100;

	private Server(DataGetter db) {
		this.db = db;
	}

	private void run() {
		try {
			serverSocket = new ServerSocket(port, maxLinker);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("can't open socket, exit.");
		}
		while (true) {
			Socket socket;
			try {
				socket = serverSocket.accept();
				new Thread(new ServerHandler(socket, db)).start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void parseArgs(String[] args) {
		try {
			if (args.length == 0 || args.length > 2)
				throw new Exception();
			port = Integer.parseInt(args[0]);
			if (port <= 1024 || port > 65535)
				throw new NumberFormatException("port not in [1025,65535]!");
			if (args.length == 2) {
				this.maxLinker = Integer.parseInt(args[1]);
				if (this.maxLinker <= 0)
					throw new NumberFormatException("maxLinker less one !");
			}
		} catch (NumberFormatException e) {
			String errMess = e.getMessage();
			if (errMess == null)
				errMess = "wrong number format!";
			System.err.println(errMess);
			System.exit(-1);
		} catch (Exception e) {
		}
	}

	/**
	 * @param args
	 *            [0] port [1] maxLinker
	 */
	public static void main(String[] args) {
		DTEventListenser listener = new ServerMaster();
		DataGetter db = new MasterDataGetter(listener);
		Server server = new Server(db);
		server.parseArgs(args);
		server.run();
	}
}
