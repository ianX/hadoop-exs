package org.rs.servernode.slave.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.servernode.protocol.Properties;

public class MFRoutines {

	public static void readMovieInfo(Path path, FileSystem fs,
			HashMap<Integer, Movie> id2Movie) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		String t;
		// String[] recs;

		try {
			while ((t = br.readLine()) != null) {
				int first = t.indexOf(',');
				int second = t.indexOf(',', first + 1);
				// recs = t.split(",");
				int id = Integer.parseInt(t.substring(0, first));
				Movie movie = new Movie(t.substring(second + 1), id);
				id2Movie.put(id, movie);
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void readMovieOrUser(Path path, FileSystem fs,
			HashMap<Integer, List<Double>> movieRec,
			HashMap<Integer, List<Double>> userRec,
			HashMap<Integer, Boolean> movieState,
			HashMap<Integer, Boolean> userState) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		String t;
		String[] recs, ratings;

		try {
			while ((t = br.readLine()) != null) {
				recs = t.split(Properties.REG_SEP_ID_VECTOR);
				assert (recs.length == 2);
				ratings = recs[1].split(Properties.REG_SEP_RATE);

				int id = Integer.parseInt(recs[0]);
				List<Double> tmpL = new ArrayList<Double>();
				for (int i = 1; i < ratings.length; i++)
					tmpL.add(Double.parseDouble(ratings[i]));

				// it is a movie
				if (recs[1].charAt(0) == '!') {
					movieRec.put(id, tmpL);
					movieState.put(id, true);
				} else {
					userRec.put(id, tmpL);
					userState.put(id, true);
				}
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void markMovieOrUser(Path path, FileSystem fs,
			HashMap<Integer, List<Double>> movieRec,
			HashMap<Integer, List<Double>> userRec,
			HashMap<Integer, Boolean> movieState,
			HashMap<Integer, Boolean> userState) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		String t;
		String[] recs;

		try {
			while ((t = br.readLine()) != null) {
				recs = t.split(Properties.REG_SEP_ID_VECTOR);
				assert (recs.length == 2);

				int id = Integer.parseInt(recs[0]);
				// it is a movie
				if (recs[1].charAt(0) == '!') {
					movieState.remove(id);
					movieState.put(id, false);
				} else {
					userState.remove(id);
					userState.put(id, false);
				}
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void removeMarkedUsers(
			HashMap<Integer, List<Double>> userRec,
			HashMap<Integer, Boolean> userState) {
		Iterator<Integer> iter = userState.keySet().iterator();
		while (iter.hasNext()) {
			Integer i = iter.next();
			if (userState.get(i).booleanValue() == false) {
				userRec.remove(i);
				iter.remove();
			}
		}
	}

	public static void removeMarkedMovies(
			HashMap<Integer, List<Double>> movieRec,
			HashMap<Integer, Boolean> movieState) {
		Iterator<Integer> iter = movieState.keySet().iterator();
		while (iter.hasNext()) {
			Integer i = iter.next();
			if (movieState.get(i).booleanValue() == false) {
				movieRec.remove(i);
				iter.remove();
			}
		}
	}

	// ret = ret + list
	public static void addList(List<Double> ret, List<Double> list) {
		for (int i = 0; i < ret.size(); i++)
			ret.set(i, ret.get(i).doubleValue() + list.get(i).doubleValue());
	}

	// ret = ret / num
	public static void aveList(List<Double> ret, int num) {
		for (int i = 0; i < ret.size(); i++)
			ret.set(i, ret.get(i).doubleValue() / num);
	}

	// return length of vector
	public static double lenList(List<Double> a) {
		double ret = 0;
		for (int i = 0; i < a.size(); i++)
			ret += a.get(i) * a.get(i);
		return Math.sqrt(ret);
	}

	// return movieVector * list
	public static double calRating(List<Double> movieVector, List<Double> list) {
		double res = 0;
		// System.out.println(movieVector.size() + " " + list.size());
		for (int i = 0; i < movieVector.size(); i++)
			res += movieVector.get(i) * list.get(i);
		return res / (lenList(movieVector) * lenList(list));
	}

	public static int getMinRatingId(List<Movie> ret) {
		int minRatingId = 0;
		for (int j = 1; j < ret.size(); j++)
			if (ret.get(j).getRating() < ret.get(minRatingId).getRating())
				minRatingId = j;
		return minRatingId;
	}

	public static int getMinSimId(List<User> ret) {
		int minRatingId = 0;
		for (int j = 1; j < ret.size(); j++)
			if (ret.get(j).getSim() < ret.get(minRatingId).getSim())
				minRatingId = j;
		return minRatingId;
	}

}
