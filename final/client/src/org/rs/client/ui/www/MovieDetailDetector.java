package org.rs.client.ui.www;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Vector;

import org.rs.object.Movie;

public class MovieDetailDetector {
	private static final String searchPrefix = "http://movie.douban.com/subject_search?search_text=";
	private static final String searchEnding = "&cat=1002";
	private static final String searchSpliter = "+";

	public static void getMovieDetails(Movie movie) {
		String name = movie.getName().replaceAll("[ ]+", searchSpliter);
		try {
			URL searchurl = new URL(searchPrefix + name + searchEnding);
			// System.out.println(name);
			HttpURLConnection con = (HttpURLConnection) searchurl
					.openConnection();
			HttpURLConnection.setFollowRedirects(true);
			con.setInstanceFollowRedirects(false);
			con.connect();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					con.getInputStream(), "UTF-8"));
			StringBuffer sb = new StringBuffer();
			String line;
			boolean start = false;
			while ((line = br.readLine()) != null) {
				if (start)
					sb.append(line);
				if (line.contains("ul first"))
					start = true;
				if (start && line.contains("</a>"))
					break;
			}
			br.close();

			// System.out.println(sb.toString());

			String movieUrl = sb.substring(sb.indexOf("href") + 6,
					sb.indexOf("\" onclick"));
			String moviePic = sb.substring(sb.indexOf("src=\"") + 5,
					sb.indexOf("\" alt")).replaceAll("spic", "lpic");

			movie.setImageURL(moviePic);
			movie.setMovieURL(movieUrl);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void getProperties(Movie movie) {
		try {
			String movieUrl = movie.getMovieURL();
			HttpURLConnection con = (HttpURLConnection) new URL(movieUrl)
					.openConnection();
			con.setInstanceFollowRedirects(false);
			con.connect();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					con.getInputStream(), "UTF-8"));
			boolean start = false;
			Vector<String> properties = new Vector<String>();
			String line;
			while ((line = br.readLine()) != null) {
				if (line.trim().isEmpty())
					continue;
				if (start && line.contains("/div"))
					break;
				if (start) {
					properties.add(line.replaceAll("<[^>]*>", "").trim());
				}
				if (line.contains("id=\"info\""))
					start = true;
			}
			br.close();

			movie.setProperties(properties.toArray(new String[0]));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
