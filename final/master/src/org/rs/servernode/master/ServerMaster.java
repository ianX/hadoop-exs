package org.rs.servernode.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

	private class Terminal implements Runnable {
		private Object mutex;

		public Terminal(Object mutex) {
			this.mutex = mutex;
		}

		@Override
		public void run() {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			try {
				String cmd;
				System.out.print("cmd: ");
				while ((cmd = br.readLine()) != null) {
					if ((cmd.startsWith("file"))) {
						String filepath = cmd
								.substring(cmd.indexOf("file") + 4).trim();
						dispatchFiles(filepath);
						synchronized (mutex) {
							ServerMaster.this.initfinished = true;
							mutex.notify();
						}
					} else if (cmd.equals("exit")) {
						// TODO:close
						System.exit(0);
					}
					System.out.print("cmd: ");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private HeartBeat heartbeat;

	private DataCombiner combiner = new MasterDataCombiner();

	private Set<Integer> registeredNode = new HashSet<Integer>();

	private int nodeNum;

	private SSPServerControl control = new SSPServerControl();

	private Map<Integer, NodeStatus> nodeStatus = new HashMap<Integer, NodeStatus>();

	private Object nodeStatusMutex = new Object();

	private Object heartBeatMutex = new Object();

	public ServerMaster(int nodeNum) {
		this.nodeNum = nodeNum;
	}

	private void dispatchFiles(String path) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] movies = fs.globStatus(new Path(path
					+ Properties.MOVIE_FILE_PREFIX));
			FileStatus[] users = fs.globStatus(new Path(path
					+ Properties.USER_FILE_PREFIX));
			String[] m = new String[movies.length];
			String[] u = new String[users.length];
			for (int i = 0; i < movies.length; i++) {
				m[i] = movies[i].getPath().toString();
			}
			for (int i = 0; i < users.length; i++) {
				u[i] = users[i].getPath().toString();
			}
			dispatch(m, u);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void dispatch(String[] movies, String[] users) throws IOException {
		int nodeNum = control.getConnected();
		if (nodeNum <= 0)
			throw new IOException("no node connceted");
		int mean = movies.length / nodeNum;
		int left = movies.length % nodeNum;
		synchronized (nodeStatusMutex) {
			int index = 0;
			for (NodeStatus node : nodeStatus.values()) {
				if (node.isAlive()) {
					for (int i = 0; i < mean; i++) {
						if (index >= movies.length)
							break;
						node.addMFile(movies[index++]);
					}
					if (left-- > 0 && index < movies.length)
						node.addMFile(movies[index++]);
				}
			}
		}
		mean = users.length / nodeNum;
		left = users.length % nodeNum;
		synchronized (nodeStatusMutex) {
			int index = 0;
			for (NodeStatus node : nodeStatus.values()) {
				if (node.isAlive()) {
					for (int i = 0; i < mean; i++) {
						if (index >= users.length)
							break;
						node.addUFile(users[index++]);
					}
					if (left-- > 0 && index < users.length)
						node.addUFile(users[index++]);
				}
			}
		}
		control.addFiles();
	}

	private boolean initfinished = false;

	private Vector<String> movies = new Vector<String>();
	private Vector<String> users = new Vector<String>();

	@Override
	public void run() {
		// TODO Auto-generated method stub
		InitAssemblage();

		Object mutex = new Object();
		new Thread(new Terminal(mutex)).start();

		synchronized (mutex) {
			while (!initfinished)
				try {
					mutex.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

		startHeartBeat();

		synchronized (heartBeatMutex) {
			while (true) {
				try {
					heartBeatMutex.wait();
					control.refreshStatus(movies, users);
					if (movies.size() == 0 && users.size() == 0)
						continue;

					try {
						dispatch(movies.toArray(new String[0]),
								users.toArray(new String[0]));
					} catch (IOException e) {
						e.printStackTrace();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void startHeartBeat() {
		heartbeat = new HeartBeat();
		heartbeat.addHeartBeatEventListener(this);
		new Thread(heartbeat).start();
	}

	@Override
	public int InitAssemblage() {
		String[] ids = Properties.NODE_IDS;
		for (int i = 0; i < nodeNum && i < ids.length; i++) {
			registeredNode.add(Integer.parseInt(ids[i]));
		}
		control.init(nodeStatus, registeredNode, nodeStatusMutex);
		return 0;
	}

	@Override
	public void handleHeartBeatEvent(HeartBeatEvent e) {
		synchronized (heartBeatMutex) {
			heartBeatMutex.notify();
		}
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
		System.out.println("rec movie");
		this.getRecMovie(event.getMovieVector(), event.getRet());
	}

	@Override
	public void handleRecUserEvent(RecUserEvent event) {
		System.out.println("rec user");
		this.getRecUser(event.getUserVector(), event.getRet());
	}

	public void getVector(Map<Movie, Integer> urating, Vector<Double> ret,
			boolean isMovie) {
		for (Movie m : urating.keySet()) {
			System.out.println(m.toString() + " " + urating.get(m));
		}
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
					mutex.wait(Properties.WAITE_TIME);
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
		List<Integer> weights = new Vector<Integer>();
		for (VectorHandler handler : vhlist) {
			if (handler.isFinished()) {
				System.out.println(handler.getRet().size());
				rets.add(handler.getRet());
				weights.add(handler.getWeight());
			}
		}

		if (isMovie) {
			combiner.combineMovieVector(rets, weights, ret);
		} else {
			combiner.combineUserVector(rets, weights, ret);
		}
		System.out.println("combine end : " + ret.size());
	}

	public void getMovieList(Vector<Movie> ret) {
		System.out.println("master :begin get movie list");
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
					mutex.wait(Properties.WAITE_TIME);
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
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		for (Movie m : ret) {
			System.out.println(m.getMid() + " " + m.toString());
		}
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		System.out.println("master: end get movie list");
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
					mutex.wait(Properties.WAITE_TIME);
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
					mutex.wait(Properties.WAITE_TIME);
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
	private int weight;
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

	public int getWeight() {
		return weight;
	}

	@Override
	public void run() {
		if (ssp.connect() != 0)
			return;
		this.weight = ssp.getVector(urating, ret, isMovie);
		this.setFinished();
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
		this.setFinished();
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
		this.setFinished();
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
		this.setFinished();
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

	public void setFinished() {
		synchronized (mutex) {
			this.finished = true;
			mutex.notify();
		}
	}
}
