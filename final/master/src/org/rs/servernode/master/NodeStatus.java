package org.rs.servernode.master;

public class NodeStatus {

	private int nodeId;
	private long lastContact = 0;

	public NodeStatus(int id) {
		// TODO Auto-generated constructor stub
		nodeId = id;
	}

	public int getNodeId() {
		return nodeId;
	}

	public void incLastContact(long unit) {
		synchronized (this) {
			lastContact += unit;
		}
	}

	public long getLastContact() {
		return lastContact;
	}

	public void resetLastContact() {
		synchronized (this) {
			lastContact = 0;
		}
	}
}
