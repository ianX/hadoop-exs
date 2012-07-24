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
	private Set<String> mfiles = new HashSet<String>();
	private Set<String> mfilesToRemove = new HashSet<String>();
	private Set<String> mfilesToAdd = new HashSet<String>();
	private Set<String> ufiles = new HashSet<String>();
	private Set<String> ufilesToRemove = new HashSet<String>();
	private Set<String> ufilesToAdd = new HashSet<String>();

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

	public Set<String> getUfiles() {
		return ufiles;
	}

	public Set<String> getMFiles() {
		return mfiles;
	}

	public void addUFile(String file) {
		this.ufilesToAdd.add(file);
	}

	public void addMFile(String file) {
		this.mfilesToAdd.add(file);
	}

	public Set<String> getUFilesToRemove() {
		return ufilesToRemove;
	}

	public Set<String> getMFilesToRemove() {
		return mfilesToRemove;
	}

	public void addUFileToRemove(String fileToRemove) {
		this.ufilesToRemove.add(fileToRemove);
	}

	public void addMFileToRemove(String fileToRemove) {
		this.mfilesToRemove.add(fileToRemove);
	}

	public Set<String> getUFilesToAdd() {
		return ufilesToAdd;
	}

	public Set<String> getMFilesToAdd() {
		return mfilesToAdd;
	}

	public void fileAdded() {
		mfiles.addAll(mfilesToAdd);
		mfilesToAdd.clear();
		ufiles.addAll(ufilesToAdd);
		ufilesToAdd.clear();
	}

	public void fileRemoved() {
		mfilesToRemove.clear();
		ufilesToRemove.clear();
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
