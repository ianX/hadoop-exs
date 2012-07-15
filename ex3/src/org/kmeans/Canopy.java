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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.kmeans.io.MovieInputFormat;

public class Canopy {

	public static final String spliter = "/";
	public static final String userSpliter = ",";
	public static final int strongMark = 8;
	public static final int weakMark = 2;

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String val = value.toString();
			if (val.contains(":")) {
				int id = Integer.parseInt(val.substring(0, val.indexOf(':'))) % 128;
				outKey.set(Integer.toString(id));
				context.write(outKey, value);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private FileSystem fs;
		private String path;
		private TreeMap<Integer, HashSet<Integer>> points = new TreeMap<Integer, HashSet<Integer>>();
		private HashMap<Integer, HashSet<Integer>> centers = new HashMap<Integer, HashSet<Integer>>();

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
			Entry<Integer, HashSet<Integer>> center;
			while ((center = points.higherEntry(key)) != null) {
				centers.put(center.getKey(), center.getValue());
				context.setStatus("choose \"mov " + center.getKey().toString()
						+ "\" as a canopy center");
				points.remove(center.getKey());
				Entry<Integer, HashSet<Integer>> point;
				while ((point = points.higherEntry(key)) != null) {
					key = point.getKey();
					tmp.clear();
					tmp.addAll(center.getValue());
					tmp.retainAll(point.getValue());
					if (tmp.size() >= strongMark)
						points.remove(key);
				}
				key = -1;
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
				HashSet<Integer> userID = new HashSet<Integer>();
				for (String uid : uids) {
					String[] u = uid.split(userSpliter);
					if (u.length == 2)
						userID.add(Integer.parseInt(u[0]));
				}
				points.put(Integer.parseInt(v[0]), userID);
				outKey.set(v[0]);
				outVal.set(v[1]);
				context.write(outKey, outVal);
			}
			context.setStatus("begin pick canopy centers");
			pickCenter(context);
			saveCenter(path + "/center-" + key.toString(), fs, this.centers);
		}

	}

	public static class MarkMap extends Mapper<Text, Text, Text, Text> {

		private HashMap<Integer, HashSet<Integer>> canopy = new HashMap<Integer, HashSet<Integer>>();
		private Set<Integer> tmpSet = new HashSet<Integer>();
		@SuppressWarnings("rawtypes")
		private MultipleOutputs mos;

		// private HashMap<String, ArrayList<Boolean>> marks = new
		// HashMap<String, ArrayList<Boolean>>();
		// private String markPath;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public void setup(Context context) throws IOException,
				InterruptedException {
			// markPath = context.getConfiguration()
			// .get("kmeans.canopy.marks", "");
			mos = new MultipleOutputs(context);
			Path[] centers = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path center : centers) {
				readCanopy(center, this.canopy);
			}
			context.setStatus("MarkMap: read canopy success");
		}

		Text outVal = new Text();

		@SuppressWarnings("unchecked")
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			HashSet<Integer> urate = new HashSet<Integer>();
			String val = value.toString();
			String[] users = val.split(spliter);
			for (String user : users) {
				String[] r = user.split(userSpliter);
				if (r.length == 2)
					urate.add(Integer.parseInt(r[0]));
			}
			long[] mark = new long[this.canopy.size() / Long.SIZE + 1];
			for (Entry<Integer, HashSet<Integer>> center : this.canopy
					.entrySet()) {
				tmpSet.clear();
				tmpSet.addAll(center.getValue());
				tmpSet.retainAll(urate);
				int i = center.getKey() / Long.SIZE;
				int j = center.getKey() % Long.SIZE;
				mark[i] &= (tmpSet.size() >= weakMark ? (1 << j) : (0 << j));
			}

			StringBuffer out = new StringBuffer();
			out.append(Integer.toString(1));
			out.append(spliter);
			out.append(Long.toString(mark[0]));
			for (int i = 1; i < mark.length; i++) {
				out.append(userSpliter);
				out.append(Long.toString(mark[i]));
			}
			out.append(spliter);
			out.append(val);
			outVal.set(out.toString());
			context.write(key, outVal);
			if (this.canopy.containsKey(Integer.parseInt(key.toString())))
				mos.write("init-center", key, outVal);
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
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

	public static void readCanopy(Path title,
			HashMap<Integer, HashSet<Integer>> canopy) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(
					title.toString()));
			String t;
			while ((t = br.readLine()) != null) {
				String[] v = t.split(spliter);
				if (v.length == 0)
					continue;
				HashSet<Integer> userID = new HashSet<Integer>();
				for (int i = 1; i < v.length; i++) {
					userID.add(Integer.parseInt(v[0]));
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

	private static void saveCenter(String path, FileSystem fs,
			HashMap<Integer, HashSet<Integer>> centers) {
		try {
			FSDataOutputStream fsds = fs.create(new Path(path));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsds));
			for (Entry<Integer, HashSet<Integer>> center : centers.entrySet()) {
				bw.append(Integer.toString(center.getKey()));
				for (Integer uid : center.getValue()) {
					bw.append(spliter + uid);
				}
				bw.newLine();
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static int mergeCanopy(String in, String out, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			HashMap<Integer, HashSet<Integer>> points = new HashMap<Integer, HashSet<Integer>>();
			HashMap<Integer, HashSet<Integer>> centers = new HashMap<Integer, HashSet<Integer>>();
			FileStatus[] files = fs.globStatus(new Path(in + "/center*"));
			for (FileStatus file : files) {
				readCanopy(file.getPath(), points);
			}

			HashSet<Integer> tmp = new HashSet<Integer>();
			HashMap<Integer, HashSet<Integer>> left = new HashMap<Integer, HashSet<Integer>>();
			while (!points.isEmpty()) {
				left.clear();
				Entry<Integer, HashSet<Integer>> center = points.entrySet()
						.iterator().next();
				centers.put(center.getKey(), center.getValue());
				points.remove(center.getKey());

				for (Entry<Integer, HashSet<Integer>> point : points.entrySet()) {
					tmp.clear();
					tmp.addAll(point.getValue());
					tmp.retainAll(center.getValue());
					if (tmp.size() < strongMark)
						left.put(point.getKey(), point.getValue());
				}
				HashMap<Integer, HashSet<Integer>> exchange = points;
				points = left;
				left = exchange;
			}

			saveCenter(out + "/marked-centers", fs, centers);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	public void markCenter(HashMap<Integer, HashMap<Integer, Integer>> centers,
			HashMap<Integer, Long[]> centerMarks) {
		int arraylen = centers.size() / Long.SIZE + 1;
		for (Entry<Integer, HashMap<Integer, Integer>> center : centers
				.entrySet()) {
			centerMarks.put(center.getKey(), new Long[arraylen]);
		}
		HashMap<Integer, HashMap<Integer, Integer>> left = new HashMap<Integer, HashMap<Integer, Integer>>();
		left.putAll(centers);
		HashSet<Integer> tmp = new HashSet<Integer>();
		while (!left.isEmpty()) {
			Integer currentKey = left.keySet().iterator().next();
			left.remove(currentKey);
			for (Entry<Integer, HashMap<Integer, Integer>> center : left
					.entrySet()) {
				tmp.clear();
				tmp.addAll(center.getValue().keySet());
				tmp.retainAll(centers.get(currentKey).keySet());

				if (tmp.size() >= 2) {
					centerMarks.get(currentKey)[center.getKey().intValue() >> 6] &= (1 << (center
							.getKey().intValue() & 63));
					centerMarks.get(currentKey)[currentKey.intValue() >> 6] &= (1 << (currentKey
							.intValue() & 63));
				} else {
					centerMarks.get(currentKey)[center.getKey().intValue() >> 6] &= (0 << (center
							.getKey().intValue() & 63));
					centerMarks.get(currentKey)[currentKey.intValue() >> 6] &= (0 << (currentKey
							.intValue() & 63));
				}
			}
		}
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

			MultipleOutputs.addNamedOutput(mark, "init-center",
					TextOutputFormat.class, Text.class, Text.class);

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
