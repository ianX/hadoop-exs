package org.rs.servernode.slave.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.servernode.protocol.Properties;

/** MSK */
public class SlaveDataLoader implements DataLoader {

	private HashMap<Integer, List<Double>> movieRec = new HashMap<Integer, List<Double>>();
	private HashMap<Integer, List<Double>> userRec = new HashMap<Integer, List<Double>>();
	private HashMap<Integer, Boolean> movieState = new HashMap<Integer, Boolean>();
	private HashMap<Integer, Boolean> userState = new HashMap<Integer, Boolean>();
	private HashMap<Integer, Movie> id2Movie = null;

	public SlaveDataLoader(String moviePath) {
		id2Movie = new HashMap<Integer, Movie>();
		Configuration conf = new Configuration();

		try {
			MFRoutines.readMovieInfo(new Path(moviePath), FileSystem.get(conf),
					id2Movie);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void addFiles(Set<String> files, boolean isMovie) {
		Configuration conf = new Configuration();
		System.out.println("adding files :");
		for (String file : files) {
			System.out.println("add file:" + file);
			try {
				MFRoutines.readMovieOrUser(new Path(file),
						FileSystem.get(conf), movieRec, userRec, movieState,
						userState);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("movie:" + movieRec.size() + "  user:"
				+ userRec.size());
	}

	@Override
	public void filesToRemove(Set<String> files) {
		Configuration conf = new Configuration();

		for (String file : files) {
			try {
				MFRoutines.markMovieOrUser(new Path(file),
						FileSystem.get(conf), movieRec, userRec, movieState,
						userState);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void removeMarkedFiles() {
		MFRoutines.removeMarkedMovies(movieRec, movieState);
		MFRoutines.removeMarkedUsers(userRec, userState);
	}

	@Override
	public int getMovieVector(Map<Movie, Integer> rating, List<Double> ret) {
		int num = 0;
		ret.clear();
		for (int i = 0; i < Properties.K; i++)
			ret.add(0.0);

		System.out.println("getting movie vector : " + rating.size());

		int mid;
		Vector<Double> v = new Vector<Double>();
		for (Movie movie : rating.keySet()) {
			mid = movie.getMid();
			int rate = rating.get(movie);
			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
					+ movie.getMid() + " " + movie.getName() + " " + rate);
			if (movieRec.containsKey(mid))
				if (movieState.get(mid).booleanValue() == true) {
					for (Double d : movieRec.get(mid)) {
						System.out.print(d + " ");
					}
					v.clear();
					v.addAll(movieRec.get(mid));
					MFRoutines.multiplyVec(v, rate);
					MFRoutines.addList(ret, v);
					num += rate;
				}
			System.out.println("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		}
		MFRoutines.aveList(ret, num);
		System.out.println("############################################");
		for (Double d : ret) {
			System.out.print(d + " ");
		}
		System.out.println("\n############################################");
		return num;
	}

	@Override
	public int getUserVector(Map<Movie, Integer> rating, List<Double> ret) {
		return getMovieVector(rating, ret);
	}

	@Override
	public void getRecMovie(List<Double> movieVector, List<Movie> ret) {
		ret.clear();

		System.out.println("getRecMovie " + "movie:" + movieRec.size()
				+ "  user:" + userRec.size());
		System.out.println("movieVector size: " + movieVector.size());
		for (Integer i : movieRec.keySet())
			if (movieState.get(i).booleanValue() == true) {
				double rating = MFRoutines.calRating(movieVector,
						movieRec.get(i));
				// System.out.println("movierec:" + i);
				if (!id2Movie.containsKey(i)) {
					continue;
				}
				id2Movie.get(i).setRating(rating);

				if (ret.size() < Properties.MAX_NUM_REC)
					ret.add(id2Movie.get(i));
				else {
					int minRatingId = MFRoutines.getMinRatingId(ret);
					if (ret.get(minRatingId).getRating() < rating)
						ret.set(minRatingId, id2Movie.get(i));
				}
			}
	}

	@Override
	public void getRecUser(List<Double> userVector, List<User> ret) {
		ret.clear();

		System.out.println("getRecUser " + "movie:" + movieRec.size()
				+ "  user:" + userRec.size());
		for (Integer i : userRec.keySet())
			if (userState.get(i).booleanValue() == true) {
				double sim = MFRoutines.calRating(userVector, userRec.get(i));

				if (ret.size() < Properties.MAX_NUM_REC)
					ret.add(new User(i.intValue(), sim));
				else {
					int minSimId = MFRoutines.getMinSimId(ret);
					if (ret.get(minSimId).getSim() < sim)
						ret.set(minSimId, new User(i.intValue(), sim));
				}
			}
	}

	// private Set<Integer> listset = new HashSet<Integer>();

	private Random rnd = new Random(System.currentTimeMillis());

	@Override
	public void getMovieList(List<Movie> ret) {
		/*
		 * ret.clear(); int size = movieRec.size();
		 * 
		 * listset.clear();
		 * 
		 * while (listset.size() < Properties.MAX_NUM_LIST) { int index; do {
		 * index = rnd.nextInt(size) + 1; } while (listset.contains(index));
		 * listset.add(index); }
		 * 
		 * for (Integer index : listset) { ret.add(id2Movie.get(index));
		 * System.out.println("getMovieList: " + id2Movie.get(index).getMid() +
		 * " " + id2Movie.get(index).toString()); }
		 */

		System.out.println("***********************************************");
		for (Integer i : movieRec.keySet()) {
			if (rnd.nextInt(Properties.RANDOM_MAX) == 0)
				if (ret.size() < Properties.MAX_NUM_LIST) {
					System.out.println("getMovieList: "
							+ id2Movie.get(i).getMid() + " "
							+ id2Movie.get(i).toString());
					ret.add(id2Movie.get(i));
				}
		}
		System.out.println("***********************************************");

	}

}
