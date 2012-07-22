package org.rs.servernode.master.event;

import java.util.EventListener;

public interface HeartBeatEventListener extends EventListener {
	public void handleHeartBeatEvent(HeartBeatEvent e);
}
