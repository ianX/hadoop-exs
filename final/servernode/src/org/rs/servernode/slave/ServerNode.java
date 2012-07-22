package org.rs.servernode.slave;

import org.rs.servernode.protocol.SSPNode;

public class ServerNode implements Slave {
	private SSPNode ssnp = new SSPNode();

	public ServerNode() {
		// TODO Auto-generated constructor stub
	}
	
	private int initNode() {
		return 0;
	}

	private int start() {
		return 0;
	}

	public static void main(String[] args) {
		ServerNode serverNode = new ServerNode();
		if (serverNode.initNode() == 0)
			serverNode.start();
	}
}
