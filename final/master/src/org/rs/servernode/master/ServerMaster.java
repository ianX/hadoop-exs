package org.rs.servernode.master;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.servernode.master.event.HeartBeatEvent;
import org.rs.servernode.master.event.ListMovieEvent;
import org.rs.servernode.master.event.RatingEvent;
import org.rs.servernode.master.event.RecMovieEvent;
import org.rs.servernode.master.event.RecUserEvent;
import org.rs.servernode.master.io.DataCombiner;
import org.rs.servernode.master.io.MasterDataCombiner;
import org.rs.servernode.protocol.NodeStatus;
import org.rs.servernode.protocol.Properties;
import org.rs.servernode.protocol.SSPServer;
import org.rs.servernode.protocol.SSPServerControl;

public class ServerMaster implements Master {

	private HeartBeat heartbeat;

	private DataCombiner combiner = new MasterDataCombiner();

	private Set<Integer> registeredNode = new HashSet<Integer>();

	private SSPServerControl control = new SSPServerControl();

	private Map<Integer, NodeStatus> nodeStatus = new HashMap<Integer, NodeStatus>();

	private Object nodeStatusMutex = new Object();

	@Override
	public void startHeartBeat() {
		heartbeat = new HeartBeat();
		heartbeat.addHeartBeatEventListener(this);
		new Thread(heartbeat).start();
	}

	@Override
	public int InitAssemblage() {
		registeredNode.add(0);
		// TODO Auto-generated method stub
		control.init(nodeStatus, registeredNode, nodeStatusMutex);
		return 0;
	}

	@Override
	public void handleHeartBeatEvent(HeartBeatEvent e) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleRatingEvent(RatingEvent event) {
		this.getVector(event.getUrating(), event.getRet(), event.isMovie());
	}

	@Override
	public void handleListMovieEvent(ListMovieEvent event) {
		this.getMovieList(event.getRet());
	}

	@Override
	public void handleRecMovieEvent(RecMovieEvent event) {
		this.getRecMovie(event.getMovieVector(), event.getRet());
	}

	@Override
	public void handleRecUserEvent(RecUserEvent event) {
		this.getRecUser(event.getUserVector(), event.getRet());
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		this.InitAssemblage();

	}

	public void getVector(Map<Movie, Integer> urating, Vector<Double> ret,
			boolean isMovie) {
		Object mutex = new Object();
		List<VectorHandler> vhlist = new Vector<VectorHandler>();
		synchronized (nodeStatusMutex) {
			for (NodeStatus node : nodeStatus.values()) {
				if (node.isAlive()) {
					VectorHandler handler = new VectorHandler(urating, isMovie,
							node.getHost(), mutex);
					vhlist.add(handler);
					new Thread(handler).start();
				}
			}
		}
		long startTime = System.currentTimeMillis();
		boolean allfinish = true;
		synchronized (mutex) {
			long ctime;
			do {
				allfinish = true;
				try {
					wait(Properties.WAITE_TIME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for (VectorHandler handler : vhlist) {
					if (!handler.isFinished()) {
						allfinish = false;
						break;
					}
				}
				ctime = System.currentTimeMillis();
			} while (ctime - startTime < Properties.WAITE_TIME && !allfinish);
		}

		List<List<Double>> rets = new Vector<List<Double>>();
		for (VectorHandler handler : vhlist) {
			if (handler.isFinished()) {
				rets.add(handler.getRet());
			}
		}
		if (isMovie) {
			combiner.combineMovieVector(rets, null, ret);
		} else {
			combiner.combineUserVector(rets, null, ret);
		}
	}

	public void getMovieList(Vector<Movie> ret) {
		Object mutex = new Object();
		List<MovieListHandler> list = new Vector<MovieListHandler>();
		synchronized (nodeStatusMutex) {
			for (NodeStatus node : nodeStatus.values()) {
				if (node.isAlive()) {
					MovieListHandler handler = new MovieListHandler(
							node.getHost(), mutex);
					list.add(handler);
					new Thread(handler).start();
				}
			}
		}
		long startTime = System.currentTimeMillis();
		boolean allfinish = true;
		synchronized (mutex) {
			long ctime;
			do {
				allfinish = true;
				try {
					wait(Properties.WAITE_TIME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for (MovieListHandler handler : list) {
					if (!handler.isFinished()) {
						allfinish = false;
						break;
					}
				}
				ctime = System.currentTimeMillis();
			} while (ctime - startTime < Properties.WAITE_TIME && !allfinish);
		}

		for (MovieListHandler handler : list) {
			if (handler.isFinished()) {
				ret.addAll(handler.getRet());
			}
		}
	}

	public void getRecMovie(Vector<Double> movieVector, Vector<Movie> ret) {
		Object mutex = new Object();
		List<RecMovieHandler> list = new Vector<RecMovieHandler>();
		synchronized (nodeStatusMutex) {
			for (NodeStatus node : nodeStatus.values()) {
				if (node.isAlive()) {
					RecMovieHandler handler = new RecMovieHandler(movieVector,
							node.getHost(), mutex);
					list.add(handler);
					new Thread(handler).start();
				}
			}
		}
		long startTime = System.currentTimeMillis();
		boolean allfinish = true;
		synchronized (mutex) {
			long ctime;
			do {
				allfinish = true;
				try {
					wait(Properties.WAITE_TIME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for (RecMovieHandler handler : list) {
					if (!handler.isFinished()) {
						allfinish = false;
						break;
					}
				}
				ctime = System.currentTimeMillis();
			} while (ctime - startTime < Properties.WAITE_TIME && !allfinish);
		}

		List<List<Movie>> rets = new Vector<List<Movie>>();
		for (RecMovieHandler handler : list) {
			if (handler.isFinished()) {
				rets.add(handler.getRet());
			}
		}
		combiner.combineRecMovie(rets, ret);
	}

	public void getRecUser(Vector<Double> userVector, Vector<User> ret) {
		Object mutex = new Object();
		List<RecUserHandler> list = new Vector<RecUserHandler>();
		synchronized (nodeStatusMutex) {
			for (NodeStatus node : nodeStatus.values()) {
				if (node.isAlive()) {
					RecUserHandler handler = new RecUserHandler(userVector,
							node.getHost(), mutex);
					list.add(handler);
					new Thread(handler).start();
				}
			}
		}
		long startTime = System.currentTimeMillis();
		boolean allfinish = true;
		synchronized (mutex) {
			long ctime;
			do {
				allfinish = true;
				try {
					wait(Properties.WAITE_TIME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for (RecUserHandler handler : list) {
					if (!handler.isFinished()) {
						allfinish = false;
						break;
					}
				}
				ctime = System.currentTimeMillis();
			} while (ctime - startTime < Properties.WAITE_TIME && !allfinish);
		}

		List<List<User>> rets = new Vector<List<User>>();
		for (RecUserHandler handler : list) {
			if (handler.isFinished()) {
				rets.add(handler.getRet());
			}
		}
		combiner.combineRecUser(rets, ret);
	}
}

class VectorHandler extends EventHandler {

	private Map<Movie, Integer> urating;
	private Vector<Double> ret;
	private boolean isMovie;

	public VectorHandler(Map<Movie, Integer> urating, boolean isMovie,
			String slavehost, Object mutex) {
		super(slavehost, mutex);
		this.urating = urating;
		this.ret = new Vector<Double>();
		this.isMovie = isMovie;
	}

	public Vector<Double> getRet() {
		return ret;
	}

	@Override
	public void run() {
		if (ssp.connect() != 0)
			return;
		ssp.getVector(urating, ret, isMovie);
		synchronized (this.getMutex()) {
			notify();
		}
	}

}

class MovieListHandler extends EventHandler {

	private Vector<Movie> ret;

	public MovieListHandler(String slavehost, Object mutex) {
		super(slavehost, mutex);
		ret = new Vector<Movie>();
	}

	public Vector<Movie> getRet() {
		return ret;
	}

	@Override
	public void run() {
		if (ssp.connect() != 0)
			return;
		ssp.getMovieList(ret);
		synchronized (this.getMutex()) {
			notify();
		}
	}

}

class RecMovieHandler extends EventHandler {

	private Vector<Double> movieVector;
	private Vector<Movie> ret;

	public RecMovieHandler(Vector<Double> movieVector, String slavehost,
			Object mutex) {
		super(slavehost, mutex);
		this.movieVector = movieVector;
		this.ret = new Vector<Movie>();
	}

	public Vector<Movie> getRet() {
		return ret;
	}

	@Override
	public void run() {
		if (ssp.connect() != 0)
			return;
		ssp.getRecMovie(movieVector, ret);
		synchronized (this.getMutex()) {
			notify();
		}
	}

}

class RecUserHandler extends EventHandler {

	private Vector<Double> userVector;
	private Vector<User> ret;

	public RecUserHandler(Vector<Double> userVector, String slavehost,
			Object mutex) {
		super(slavehost, mutex);
		this.userVector = userVector;
		this.ret = new Vector<User>();
	}

	public Vector<User> getRet() {
		return ret;
	}

	@Override
	public void run() {
		if (ssp.connect() != 0)
			return;
		ssp.getRecUser(userVector, ret);
		synchronized (this.getMutex()) {
			notify();
		}
	}

}

abstract class EventHandler implements Runnable {
	private boolean finished = false;
	private Object mutex;
	SSPServer ssp;

	public EventHandler() {
	}

	public EventHandler(String slavehost, Object mutex) {
		this.mutex = mutex;
		this.ssp = new SSPServer(slavehost);
	}

	public Object getMutex() {
		return mutex;
	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}
}
