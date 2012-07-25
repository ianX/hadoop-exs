package org.rs.servernode.protocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

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

			boolean needDispatch = false;
			synchronized (mutex) {
				if (nodeStatus.containsKey(nodeid)) {
					needDispatch = true;
					node = nodeStatus.get(nodeid);
					node.setOos(oos);
					node.setAlive(true);
					Set<String> movies = node.getMFiles();
					Set<String> users = node.getUFiles();
					node.getMFilesToAdd().clear();
					node.getUFilesToAdd().clear();
					node.getMFilesToAdd().addAll(movies);
					node.getUFilesToAdd().addAll(users);
					for (String m : movies) {
						for (NodeStatus node : nodeStatus.values()) {
							if (node.getMFiles().contains(m))
								node.addMFileToRemove(m);
						}
					}
					for (String u : users) {
						for (NodeStatus node : nodeStatus.values()) {
							if (node.getUFiles().contains(u))
								node.addUFileToRemove(u);
						}
					}
				} else {
					node = new NodeStatus(nodeid, oos);
					nodeStatus.put(nodeid, node);
				}
				node.setHost(socket.getInetAddress().getHostAddress());
				connected++;
			}

			if (needDispatch) {
				SSPServerControl.this.addFiles();
				SSPServerControl.this.filesToRemove();
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

	public void refreshStatus(Vector<String> movies, Vector<String> users) {
		movies.clear();
		users.clear();
		synchronized (mutex) {
			for (NodeStatus node : nodeStatus.values()) {
				if (node.getLastContact() >= Properties.NODE_DEAD) {
					node.setAlive(false);
					movies.addAll(node.getMFiles());
					movies.addAll(node.getMFilesToAdd());
					users.addAll(node.getUFiles());
					users.addAll(node.getUFilesToAdd());
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
				if (node.getMFilesToAdd().size() == 0
						&& node.getUFilesToAdd().size() == 0)
					continue;
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
				if (node.getMFilesToRemove().size() == 0
						&& node.getUFilesToRemove().size() == 0)
					continue;
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
