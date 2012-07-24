package org.rs.servernode.master.io;

import java.util.List;

import org.rs.object.Movie;
import org.rs.object.User;
import org.rs.servernode.protocol.Properties;

/** MSK */
public class MasterDataCombiner implements DataCombiner {

	public void multiplyVec(List<Double> a, double c) {
		for (int i = 0; i < a.size(); i++)
			a.set(i, a.get(i) * c);
	}

	public void addVec(List<Double> a, List<Double> b) {
		for (int i = 0; i < a.size(); i++)
			a.set(i, a.get(i) + b.get(i));
	}

	@Override
	public void combineMovieVector(List<List<Double>> movieVectors,
			List<Integer> counts, List<Double> ret) {

		System.out.println("combine");

		ret.clear();
		// zero vector with length MyParm.K
		for (int i = 0; i < Properties.K; i++)
			ret.add(0.0);

		double total = 0;
		for (int i = 0; i < movieVectors.size(); i++) {
			System.out.println(movieVectors.get(i).size());
			multiplyVec(movieVectors.get(i), counts.get(i));
			total += counts.get(i);
			addVec(ret, movieVectors.get(i));
		}
		// calc average
		multiplyVec(ret, 1 / total);
		System.out.println(ret.size());
		System.out.println("combine end");
	}

	@Override
	public void combineUserVector(List<List<Double>> movieVectors,
			List<Integer> counts, List<Double> ret) {
		combineMovieVector(movieVectors, counts, ret);
	}

	@Override
	public void combineRecMovie(List<List<Movie>> recMovies, List<Movie> ret) {

		ret.clear();
		for (int i = 0; i < Properties.MAX_NUM_REC; i++) {
			Movie tmp = null;
			for (int j = 0; j < recMovies.size(); j++)
				for (int k = 0; k < recMovies.get(j).size(); k++)
					if (recMovies.get(j).get(k).getRating() > 0)
						if (tmp == null
								|| recMovies.get(j).get(k).getRating() > tmp
										.getRating())
							tmp = recMovies.get(j).get(k);

			// select tmp, delete tmp from recMovies
			if (tmp == null)
				break;
			tmp.setRating(-1);
			ret.add(tmp);
		}
	}

	@Override
	public void combineRecUser(List<List<User>> recUsers, List<User> ret) {
		ret.clear();
		for (int i = 0; i < Properties.MAX_NUM_REC; i++) {
			User tmp = null;
			for (int j = 0; j < recUsers.size(); j++)
				for (int k = 0; k < recUsers.get(j).size(); k++)
					if (recUsers.get(j).get(k).getSim() > 0)
						if (tmp == null
								|| recUsers.get(j).get(k).getSim() > tmp
										.getSim())
							tmp = recUsers.get(j).get(k);

			// select tmp, delete tmp from recMovies
			if (tmp == null)
				break;
			tmp.setSim(-1);
			ret.add(tmp);
		}
	}
}
