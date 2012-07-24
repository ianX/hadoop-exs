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
			this.socket = socket;
		}

		private void nodeDead() {
			synchronized (mutex) {
				node.setAlive(false);
				try {
					node.getOos().close();
					this.ois.close();
					this.socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void run() {
			int nodeid = -1;
			try {
				oos = new ObjectOutputStream(socket.getOutputStream());
				ois = new ObjectInputStream(socket.getInputStream());
				nodeid = ois.readInt();
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			if (nodeid == -1)
				return;

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

			synchronized (SSPServerControl.this) {
				SSPServerControl.this.notify();
			}

			while (true) {
				try {
					int hb = ois.readInt();
					if (hb != Properties.HEART_BEAT) {
						this.nodeDead();
						return;
					}

					synchronized (mutex) {
						node.resetLastContact();
						if (!node.isAlive()) {
							this.nodeDead();
							return;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
					this.nodeDead();
					return;
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
		synchronized (mutex) {
			for (NodeStatus node : nodeStatus.values()) {
				if (node.getLastContact() >= Properties.NODE_DEAD) {
					node.setAlive(false);
					connected--;
				}
			}
		}
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
				while (connected < registeredNode.size() * 0.7)
					this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("control init finish");
	}

	@Override
	public void run() {
		try {
			masterSocket = new ServerSocket(Properties.MASTER_PORT, 4000);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		while (true) {
			Socket socket;
			try {
				socket = masterSocket.accept();
				new Thread(new SlaveConnectHandler(socket)).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
			synchronized (this) {
				this.notify();
			}
		}
	}

	public void addFiles() {
		synchronized (mutex) {
			for (NodeStatus node : nodeStatus.values()) {
				try {
					System.out.println(node.getMFilesToAdd().size());
					System.out.println(node.getUFilesToAdd().size());
					ObjectOutputStream oos = node.getOos();

					oos.writeInt(Properties.ADD_FILES);
					oos.writeObject(node.getMFilesToAdd());
					oos.writeBoolean(true);
					oos.writeInt(Properties.ADD_FILES);
					oos.writeObject(node.getUFilesToAdd());
					oos.writeBoolean(false);
					oos.flush();
					node.fileAdded();
				} catch (IOException e) {
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
					oos.writeObject(node.getMFilesToRemove());
					oos.writeBoolean(true);
					oos.writeInt(Properties.FILES_TO_REOMVE);
					oos.writeObject(node.getUFilesToRemove());
					oos.writeBoolean(false);
					oos.flush();
				} catch (IOException e) {
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
					e.printStackTrace();
				}
			}
		}
	}
}
