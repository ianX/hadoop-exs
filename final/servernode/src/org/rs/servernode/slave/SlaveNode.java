package org.rs.servernode.slave;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.rs.servernode.protocol.Properties;
import org.rs.servernode.slave.io.SlaveDataLoader;

public class SlaveNode implements Slave {

	private class AddFilesHandler implements Runnable {

		private Set<String> files;
		private boolean isMovie;

		public AddFilesHandler(Set<String> files, boolean isMovie) {
			this.files = files;
			this.isMovie = isMovie;
		}

		@Override
		public void run() {
			dataLoader.addFiles(files, isMovie);
		}
	}

	private class FilesToRemoveHandler implements Runnable {

		private Set<String> files;

		public FilesToRemoveHandler(Set<String> files) {
			this.files = files;
		}

		@Override
		public void run() {
			dataLoader.filesToRemove(files);
		}

	}

	private class RemoveMarkedFiles implements Runnable {

		@Override
		public void run() {
			dataLoader.removeMarkedFiles();
		}
	}

	private class HeartBeat implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					oos.writeInt(Properties.HEART_BEAT);
					oos.flush();
					Thread.sleep(Properties.SLEEP_TIME);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	private Socket socket;
	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private String host;
	private int id;

	private SlaveDataLoader dataLoader = new SlaveDataLoader();
	private SlaveCmdHandler cmdHandler;

	public SlaveNode(String host, int id) {
		this.host = host;
		this.id = id;
	}

	private int initNode() {
		try {
			socket = new Socket(host, Properties.MASTER_PORT);
			oos = new ObjectOutputStream(socket.getOutputStream());
			ois = new ObjectInputStream(socket.getInputStream());
			oos.writeInt(id);
			oos.flush();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	private void start() {
		new Thread(new HeartBeat()).start();

		cmdHandler = new SlaveCmdHandler(dataLoader);
		new Thread(cmdHandler).start();

		int cmd;
		while (true) {
			try {
				cmd = ois.readInt();
				switch (cmd) {
				case Properties.ADD_FILES:
					final Set<String> files = (HashSet<String>) ois
							.readObject();
					final boolean isMovie = ois.readBoolean();
					new Thread(new AddFilesHandler(files, isMovie)).start();
					break;
				case Properties.FILES_TO_REOMVE:
					final Set<String> filesToRemove = (HashSet<String>) ois
							.readObject();
					new Thread(new FilesToRemoveHandler(filesToRemove)).start();
					break;
				case Properties.REMOVE_MARKED_FILES:
					new Thread(new RemoveMarkedFiles()).start();
					break;
				default:
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		if (args.length != 2)
			System.out.println("usage : servernode masterHost nodeID");
		SlaveNode slaveNode = new SlaveNode(args[0], Integer.parseInt(args[2]));
		if (slaveNode.initNode() == 0)
			slaveNode.start();
		System.out.println("connect error");
	}
}
