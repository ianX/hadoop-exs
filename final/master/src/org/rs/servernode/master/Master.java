package org.rs.servernode.master;

import org.rs.servernode.master.event.DTEventListenser;
import org.rs.servernode.master.event.HeartBeatEventListener;

public interface Master extends HeartBeatEventListener, DTEventListenser {

	public void startHeartBeat();

	public int InitAssemblage();
}
