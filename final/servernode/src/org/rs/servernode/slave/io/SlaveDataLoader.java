package org.rs.servernode.slave.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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

		int mid;
		for (Movie movie : rating.keySet()) {
			mid = movie.getMid();
			if (movieRec.containsKey(mid))
				if (movieState.get(mid).booleanValue() == true) {
					MFRoutines.addList(ret, movieRec.get(mid));
					num++;
				}
		}
		MFRoutines.aveList(ret, num);
		return num;
	}

	@Override
	public int getUserVector(Map<Movie, Integer> rating, List<Double> ret) {
		return getMovieVector(rating, ret);
	}

	@Override
	public void getRecMovie(List<Double> movieVector, List<Movie> ret) {
		ret.clear();

		System.out.println("movieVector size: " + movieVector.size());
		for (Integer i : movieRec.keySet())
			if (movieState.get(i).booleanValue() == true) {
				double rating = MFRoutines.calRating(movieVector,
						movieRec.get(i));
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

	@Override
	public void getMovieList(List<Movie> ret) {
		ret.clear();
		Random rnd = new Random(System.currentTimeMillis());
		for (Integer i : movieRec.keySet())
			if (rnd.nextInt(Properties.RANDOM_MAX) == 0)
				if (ret.size() < Properties.MAX_NUM_LIST)
					ret.add(id2Movie.get(i));
	}

}
