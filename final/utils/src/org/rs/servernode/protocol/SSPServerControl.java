package org.rs.servernode.protocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Set;

public class SSPServerControl implements Runnable {

	private class SlaveConnectHandler implements Runnable {

		private Socket socket;
		private ObjectInputStream ois;
		private ObjectOutputStream oos;
		private NodeStatus node;

		public SlaveConnectHandler(Socket socket) {
			// TODO Auto-generated constructor stub
			this.socket = socket;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			int nodeid = -1;
			try {
				oos = new ObjectOutputStream(socket.getOutputStream());
				ois = new ObjectInputStream(socket.getInputStream());
				nodeid = ois.readInt();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}

			synchronized (mutex) {
				if (nodeStatus.containsKey(nodeid)) {
					node = nodeStatus.get(nodeid);
					node.setOos(oos);
				} else {
					node = new NodeStatus(nodeid, oos);
					nodeStatus.put(nodeid, node);
				}
				connected++;
			}

			while (true) {
				try {
					int hb = ois.readInt();
					if (hb != Properties.HEART_BEAT) {
						this.ois.close();
						this.socket.close();
						return;
					}
					synchronized (mutex) {
						node.resetLastContact();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	private ServerSocket masterSocket;
	private Map<Integer, NodeStatus> nodeStatus;
	Set<Integer> registeredNode;
	private int connected = 0;
	private Object mutex;

	public void refreshStatus() {

	}

	public int getConnected() {
		return connected;
	}

	public void init(Map<Integer, NodeStatus> nodeStatus,
			Set<Integer> registeredNode, Object mutex) {
		this.registeredNode = registeredNode;
		this.nodeStatus = nodeStatus;
		this.mutex = mutex;
		new Thread(this).start();
		synchronized (this) {
			try {
				while (connected < registeredNode.size() * 0.6)
					wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			masterSocket = new ServerSocket(Properties.MASTER_PORT, 4000);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		while (true) {
			Socket socket;
			try {
				socket = masterSocket.accept();
				new Thread(new SlaveConnectHandler(socket)).start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			synchronized (this) {
				notify();
			}
		}
	}

	public void addFiles() {
		synchronized (mutex) {
			for (NodeStatus node : nodeStatus.values()) {
				try {
					ObjectOutputStream oos = node.getOos();
					oos.writeInt(Properties.ADD_FILES);
					oos.writeObject(node.getFilesToAdd());
					oos.flush();
					node.fileAdded();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void filesToRemove() {
		synchronized (mutex) {
			for (NodeStatus node : nodeStatus.values()) {
				try {
					ObjectOutputStream oos = node.getOos();
					oos.writeInt(Properties.FILES_TO_REOMVE);
					oos.writeObject(node.getFilesToRemove());
					oos.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void removeMarkedFiles() {
		synchronized (mutex) {
			for (NodeStatus node : nodeStatus.values()) {
				try {
					ObjectOutputStream oos = node.getOos();
					oos.writeInt(Properties.REMOVE_MARKED_FILES);
					oos.flush();
					node.fileRemoved();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}