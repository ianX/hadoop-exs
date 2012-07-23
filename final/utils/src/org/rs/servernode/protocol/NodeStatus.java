package org.rs.servernode.protocol;

import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

public class NodeStatus {

	private int nodeId;
	private long lastContact = 0;

	private String host;

	private boolean alive = true;

	private ObjectOutputStream oos;
	private Set<String> files = new HashSet<String>();
	private Set<String> filesToRemove = new HashSet<String>();
	private Set<String> filesToAdd = new HashSet<String>();

	public NodeStatus(int id, ObjectOutputStream oos) {
		nodeId = id;
		this.oos = oos;
	}

	public boolean isAlive() {
		return alive;
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getHost() {
		return host;
	}

	public Set<String> getFiles() {
		return files;
	}

	public void addFile(String file) {
		this.filesToAdd.add(file);
	}

	public Set<String> getFilesToRemove() {
		return filesToRemove;
	}

	public void addFileToRemove(String fileToRemove) {
		this.filesToRemove.add(fileToRemove);
	}

	public Set<String> getFilesToAdd() {
		return filesToAdd;
	}

	public void fileAdded() {
		files.addAll(filesToAdd);
		filesToAdd.clear();
	}

	public void fileRemoved() {
		filesToRemove.clear();
	}

	public void setOos(ObjectOutputStream oos) {
		this.oos = oos;
	}

	public ObjectOutputStream getOos() {
		return oos;
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

	@Override
	public int hashCode() {
		return this.nodeId;
	}

	@Override
	public boolean equals(Object obj) {
		return this.nodeId == ((NodeStatus) obj).nodeId;
	}
}
