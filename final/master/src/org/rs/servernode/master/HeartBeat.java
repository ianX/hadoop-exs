package org.rs.servernode.master;

import java.util.Vector;

import org.rs.servernode.master.event.HeartBeatEvent;
import org.rs.servernode.master.event.HeartBeatEventListener;

public class HeartBeat implements Runnable {
	private Vector<HeartBeatEventListener> listeners = new Vector<HeartBeatEventListener>();
	private int heartbeat = 1000;

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			try {
				this.notifyHeartBeatEventListener();
				Thread.sleep(heartbeat);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
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
