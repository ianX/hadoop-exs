package org.rs.servernode.master.event;

import java.util.EventObject;

import org.rs.servernode.master.HeartBeat;

public class HeartBeatEvent extends EventObject {

	private static final long serialVersionUID = 6107215555591417758L;

	private HeartBeat source;

	public HeartBeatEvent(HeartBeat source) {
		super(source);
		this.source = source;
	}

	public HeartBeat getSource() {
		return source;
	}
}
