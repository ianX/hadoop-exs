package org.rs.servernode.master;

import org.rs.servernode.master.event.DBEventListenser;
import org.rs.servernode.master.event.HeartBeatEventListener;

public interface Master extends HeartBeatEventListener, DBEventListenser {

	public void startHeartBeat();

	public int InitAssemblage();
}
