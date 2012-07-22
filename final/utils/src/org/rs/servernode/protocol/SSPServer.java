package org.rs.servernode.protocol;

public class SSPServer implements Runnable {

	private Object mutex;

	public SSPServer(Object mutex) {
		// TODO Auto-generated constructor stub
		this.mutex = mutex;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		synchronized (mutex) {
			
		}
	}

}
