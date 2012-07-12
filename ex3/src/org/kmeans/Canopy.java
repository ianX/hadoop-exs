package org.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.kmeans.io.MovieInputFormat;

public class Canopy {
	private static String spliter = "/";

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String val = value.toString();
			if (val.contains(":")) {
				int id = Integer.parseInt(val.substring(0, val.indexOf(':'))) % 64;
				outKey.set(Integer.toString(id));
				context.write(outKey, value);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private FileSystem fs;
		private String path;
		private TreeMap<Integer, HashMap<Integer, Integer>> points = new TreeMap<Integer, HashMap<Integer, Integer>>();
		private HashMap<Integer, HashMap<Integer, Integer>> centers = new HashMap<Integer, HashMap<Integer, Integer>>();

		private Text outKey = new Text();
		private Text outVal = new Text();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			fs = FileSystem.get(context.getConfiguration());
			path = context.getConfiguration().get("kmeans.canopy.centers", "");
		}

		private void pickCenter(Context context) {
			HashSet<Integer> tmp = new HashSet<Integer>();
			Integer key = -1;
			Entry<Integer, HashMap<Integer, Integer>> center;
			while ((center = points.higherEntry(key)) != null) {
				centers.put(center.getKey(), center.getValue());
				points.remove(center.getKey());
				Entry<Integer, HashMap<Integer, Integer>> point;
				while ((point = points.higherEntry(key)) != null) {
					key = point.getKey();
					tmp.clear();
					tmp.addAll(center.getValue().keySet());
					tmp.retainAll(point.getValue().keySet());
					if (tmp.size() >= 8)
						points.remove(key);
				}
				key = -1;
			}
		}

		private void saveCenter(String fid) {
			try {
				FSDataOutputStream fsds = fs.create(new Path(path + "/center-"
						+ fid));
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
						fsds));
				for (Entry<Integer, HashMap<Integer, Integer>> center : centers
						.entrySet()) {
					bw.append(Integer.toString(center.getKey()));
					for (Entry<Integer, Integer> uid : center.getValue()
							.entrySet()) {
						bw.append(spliter + uid.getKey() + "," + uid.getValue());
					}
					bw.newLine();
				}
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			points.clear();
			centers.clear();
			for (Text val : value) {
				String[] v = val.toString().split(":");
				if (v.length != 2)
					continue;
				String[] uids = v[1].split(spliter);
				HashMap<Integer, Integer> userID = new HashMap<Integer, Integer>();
				for (String uid : uids) {
					String[] u = uid.split(",");
					if (u.length == 2)
						userID.put(Integer.parseInt(u[0]),
								Integer.parseInt(u[1]));
				}
				points.put(Integer.parseInt(v[0]), userID);
				outKey.set(v[0]);
				outVal.set(v[1]);
				context.write(outKey, outVal);
			}
			pickCenter(context);
			saveCenter(key.toString());
		}

	}

	public static class MarkMap extends Mapper<Text, Text, Text, Text> {

		private HashMap<Integer, HashMap<Integer, Integer>> canopy = new HashMap<Integer, HashMap<Integer, Integer>>();
		private Set<Integer> tmpSet = new HashSet<Integer>();

		// private HashMap<String, ArrayList<Boolean>> marks = new
		// HashMap<String, ArrayList<Boolean>>();
		// private String markPath;

		public void setup(Context context) throws IOException,
				InterruptedException {
			// markPath = context.getConfiguration()
			// .get("kmeans.canopy.marks", "");
			Path[] centers = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path center : centers) {
				readCanopy(center, this.canopy);
			}
		}

		Text outVal = new Text();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			HashMap<Integer, Integer> urate = new HashMap<Integer, Integer>();
			String[] users = value.toString().split(spliter);
			for (String user : users) {
				String[] r = user.split(",");
				if (r.length == 2)
					urate.put(Integer.parseInt(r[0]), Integer.parseInt(r[1]));
			}
			byte[] mark = new byte[this.canopy.size()];
			for (Entry<Integer, HashMap<Integer, Integer>> center : this.canopy
					.entrySet()) {
				tmpSet.clear();
				tmpSet.addAll(center.getValue().keySet());
				tmpSet.retainAll(urate.keySet());
				if (tmpSet.size() >= 2)
					mark[center.getKey()] = '1';
				else
					mark[center.getKey()] = '0';
			}
			outVal.set(mark);
			context.write(key, outVal);
		}

		/*
		 * private void writeMarks(Context context) { try { FSDataOutputStream
		 * fsds = FileSystem.get( context.getConfiguration()).create( new
		 * Path(this.markPath + "/mark-m-" +
		 * context.getTaskAttemptID().getId())); BufferedWriter bw = new
		 * BufferedWriter(new OutputStreamWriter( fsds)); for (Entry<String,
		 * ArrayList<Boolean>> mark : marks.entrySet()) {
		 * bw.append(mark.getKey() + "\t"); for (Boolean m : mark.getValue()) {
		 * bw.append(m ? "1" : "0"); } bw.newLine(); } bw.close(); } catch
		 * (IOException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } }
		 * 
		 * 
		 * @Override public void cleanup(Context context) throws IOException,
		 * InterruptedException { writeMarks(context); }
		 */
	}

	private static void readCanopy(Path title,
			HashMap<Integer, HashMap<Integer, Integer>> canopy) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(
					title.toString()));
			String t;
			while ((t = br.readLine()) != null) {
				String[] v = t.split(spliter);
				if (v.length == 0)
					continue;
				HashMap<Integer, Integer> userID = new HashMap<Integer, Integer>();
				for (int i = 1; i < v.length; i++) {
					String[] u = v[i].split(",");
					if (u.length == 2)
						userID.put(Integer.parseInt(u[0]),
								Integer.parseInt(u[1]));
				}
				canopy.put(Integer.parseInt(v[0]), userID);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static int mergeCanopy(String in, String out, Configuration conf) {
		try {
			HashMap<Integer, HashMap<Integer, Integer>> points = new HashMap<Integer, HashMap<Integer, Integer>>();
			HashMap<Integer, HashMap<Integer, Integer>> centers = new HashMap<Integer, HashMap<Integer, Integer>>();
			FileStatus[] files = FileSystem.get(conf).globStatus(
					new Path(in + "/center*"));
			for (FileStatus file : files) {
				readCanopy(file.getPath(), points);
			}

			HashSet<Integer> tmp = new HashSet<Integer>();
			HashMap<Integer, HashMap<Integer, Integer>> leftpoints = new HashMap<Integer, HashMap<Integer, Integer>>();
			while (!points.isEmpty()) {
				leftpoints.clear();
				Entry<Integer, HashMap<Integer, Integer>> center = points
						.entrySet().iterator().next();
				centers.put(center.getKey(), center.getValue());
				points.remove(center.getKey());

				for (Entry<Integer, HashMap<Integer, Integer>> point : points
						.entrySet()) {
					tmp.clear();
					tmp.addAll(point.getValue().keySet());
					tmp.retainAll(center.getValue().keySet());
					if (tmp.size() < 8)
						leftpoints.put(point.getKey(), point.getValue());
				}
				HashMap<Integer, HashMap<Integer, Integer>> exchange = points;
				points = leftpoints;
				leftpoints = exchange;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	/**
	 * args[0]: input ; args[1]: output ; args[2]: marks ; args[3]: canopys .
	 */
	public static int run(String[] args, Configuration conf) {
		try {
			Job job = new Job(conf, "canopy");
			job.setJarByClass(Canopy.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(MovieInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			String cpath = args[0] + "_centers";
			job.getConfiguration().set("kmeans.canopy.centers", cpath);

			job.waitForCompletion(true);

			if (mergeCanopy(cpath, args[3], job.getConfiguration()) != 0)
				return -1;

			Job mark = new Job(conf, "mark");
			mark.setJarByClass(Canopy.class);
			mark.setMapperClass(MarkMap.class);
			mark.setOutputKeyClass(Text.class);
			mark.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(mark, new Path(args[1]));
			FileOutputFormat.setOutputPath(mark, new Path(args[2]));

			DistributedCache.addCacheFile(new Path(args[3]).toUri(),
					mark.getConfiguration());

			mark.waitForCompletion(true);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
}
