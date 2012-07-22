package org.rs.object;

import java.io.Serializable;

public class Movie implements Serializable, Comparable<Movie> {
	private static final long serialVersionUID = -4543712156703334945L;
	private String name;
	private int mid = -1;
	private int marking = 0;
	private String imageURL = null;
	private String movieURL = null;
	private String[] properties = null;

	public Movie(String name) {
		this.name = name;
	}

	public Movie(String name, int mid) {
		// TODO Auto-generated constructor stub
		this.name = name;
		this.mid = mid;
	}

	public void setMarking(int marking) {
		this.marking = marking;
	}

	public int getMarking() {
		return marking;
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
		// TODO Auto-generated method stub
		Movie m = (Movie) obj;
		return m.getMid() == mid || name.equals(m.getName());
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return name.hashCode();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return name;
	}

	@Override
	public int compareTo(Movie o) {
		// TODO Auto-generated method stub
		return this.getMarking() - o.getMarking();
	}
}
