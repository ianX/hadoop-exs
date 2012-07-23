package org.rs.servernode.master;

import java.util.Vector;

import org.rs.servernode.master.event.HeartBeatEvent;
import org.rs.servernode.master.event.HeartBeatEventListener;
import org.rs.servernode.protocol.Properties;

public class HeartBeat implements Runnable {
	private Vector<HeartBeatEventListener> listeners = new Vector<HeartBeatEventListener>();

	@Override
	public void run() {
		while (true) {
			try {
				this.notifyHeartBeatEventListener();
				Thread.sleep(Properties.SLEEP_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void addHeartBeatEventListener(HeartBeatEventListener listener) {
		this.listeners.add(listener);
	}

	public void removeHeartBeatEventListener(HeartBeatEventListener listener) {
		this.listeners.remove(listener);
	}

	public void notifyHeartBeatEventListener() {
		for (HeartBeatEventListener listener : listeners) {
			listener.handleHeartBeatEvent(new HeartBeatEvent(this));
		}
	}
}
