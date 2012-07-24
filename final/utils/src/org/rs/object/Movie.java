package org.rs.object;

import java.io.Serializable;

public class Movie implements Serializable, Comparable<Movie> {
	private static final long serialVersionUID = 6054453888751836297L;
	private String name = null;
	private int mid = -1;
	private double rating = 0;
	private String imageURL = null;
	private String movieURL = null;
	private String[] properties = null;

	public Movie(int id) {
		this.mid = id;
	}

	public Movie(String name) {
		this.name = name;
	}

	public Movie(String name, int mid) {
		this.name = name;
		this.mid = mid;
	}

	public void setRating(double rating) {
		this.rating = rating;
	}

	public double getRating() {
		return rating;
	}

	public void setMovieURL(String movieURL) {
		this.movieURL = movieURL;
	}

	public String getMovieURL() {
		return movieURL;
	}

	public String getImageURL() {
		return imageURL;
	}

	public void setImageURL(String imageURL) {
		this.imageURL = imageURL;
	}

	public void setProperties(String[] properties) {
		this.properties = properties;
	}

	public String[] getProperties() {
		return properties;
	}

	public String getName() {
		return name;
	}

	public int getMid() {
		return mid;
	}

	@Override
	public boolean equals(Object obj) {
		Movie m = (Movie) obj;
		return m.getMid() == mid || name.equals(m.getName());
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public String toString() {
		if (name != null)
			return name;
		else
			return Integer.toString(mid);
	}

	@Override
	public int compareTo(Movie o) {
		double diff = this.getRating() - o.getRating();
		return diff > 0 ? 1 : (diff < 0 ? -1 : 0);
	}
}
