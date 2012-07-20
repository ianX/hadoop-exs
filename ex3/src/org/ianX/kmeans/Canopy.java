package org.ianX.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.ianX.util.StringSpliter;

public class Canopy {

	/**
	 * input : one movie(value[mid:u1,r1/u2,r2...]); output key is a reduce id,
	 * output value is the input movie
	 */
	public static class Map extends Mapper<Text, Text, IntWritable, Text> {

		private IntWritable outKey = new IntWritable();
		private Text outVal = new Text();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			int id = Integer.parseInt(key.toString()) & 127;
			outKey.set(id);
			outVal.set(key.toString() + Constants.mrSpliter + value.toString());
			context.write(outKey, outVal);
		}
	}

	/**
	 * input key is the reduce id,value are movies; output key is the movie id,
	 * output value is a text like "u1,r1/u2,r2...". mos output is canopy
	 * centers: id u1/u2/u3...
	 */
	public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

		private FileSystem fs;
		private String path;

		private Text outKey = new Text();
		private Text outVal = new Text();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			fs = FileSystem.get(context.getConfiguration());
			path = context.getConfiguration().get(
					Constants.KMEANS_CANOPY_CENTERS, "");
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
					if (tmp.size() >= Constants.strongMark)
						points.remove(key);
				}
				key = -1;
			}
		}

		private StringSpliter sspliter = new StringSpliter();

		private TreeMap<Integer, HashSet<Integer>> points = new TreeMap<Integer, HashSet<Integer>>();
		private HashMap<Integer, HashSet<Integer>> centers = new HashMap<Integer, HashSet<Integer>>();

		public void reduce(IntWritable key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			points.clear();
			centers.clear();

			for (Text val : value) {
				sspliter.set(val.toString(), Constants.mrSpliter);
				String outkey = sspliter.next();
				String outval = sspliter.left();
				if (outkey == null || outval == null)
					continue;

				sspliter.set(outval, Constants.userSpliter);
				HashSet<Integer> userID = new HashSet<Integer>();
				String uid;
				while ((uid = sspliter.next()) != null) {
					sspliter.changeSpliter(Constants.spliter);
					String urate = sspliter.next();
					sspliter.changeSpliter(Constants.userSpliter);

					if (urate == null)
						break;
					userID.add(Integer.parseInt(uid));
				}
				points.put(Integer.parseInt(outkey), userID);
				outKey.set(outkey);
				outVal.set(outval);
				context.write(outKey, outVal);
			}
			context.setStatus("begin pick canopy centers");
			pickCenter(context);
			saveCanopyCenter(
					path + "/" + Constants.CanopyPrefix + "-" + key.get(), fs,
					this.centers);
		}

	}

	/**
	 * Mark data set by canopy. input key is movie id, value is u1,r1/u2,r2...;
	 * output key is movie id, value is marked rating with default center:
	 * 1/m1,m2../u1,r1/u2,r2/...
	 */
	public static class MarkMap extends Mapper<Text, Text, Text, Text> {

		private HashMap<Integer, HashSet<Integer>> canopy = new HashMap<Integer, HashSet<Integer>>();
		private HashSet<Integer> cid = new HashSet<Integer>();
		private Set<Integer> tmpSet = new HashSet<Integer>();
		@SuppressWarnings("rawtypes")
		private MultipleOutputs mos;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public void setup(Context context) throws IOException,
				InterruptedException {
			String cidPath = context.getConfiguration().get(
					Constants.KMEANS_CANOPY_MOVIEID);
			ObjectInputStream ois = new ObjectInputStream(FileSystem.get(
					context.getConfiguration()).open(new Path(cidPath)));
			try {
				cid = (HashSet<Integer>) ois.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ois.close();

			mos = new MultipleOutputs(context);
			Path[] centers = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path center : centers) {
				BufferedReader br = new BufferedReader(new FileReader(
						center.toString()));
				readCanopyCenter(br, canopy);
			}
			context.setStatus("MarkMap: read canopy success");
		}

		private Text outVal = new Text();
		private StringSpliter sspliter = new StringSpliter();

		private HashSet<Integer> urate = new HashSet<Integer>();

		@SuppressWarnings("unchecked")
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			urate.clear();

			boolean iscc = false;
			String val = value.toString();

			sspliter.set(val, Constants.userSpliter);
			String uid;
			while ((uid = sspliter.next()) != null) {
				sspliter.changeSpliter(Constants.spliter);
				String u = sspliter.next();
				sspliter.changeSpliter(Constants.userSpliter);
				if (u == null)
					break;
				urate.add(Integer.parseInt(uid));
			}

			long[] mark = new long[this.canopy.size() / Long.SIZE + 1];
			for (Entry<Integer, HashSet<Integer>> cc : this.canopy.entrySet()) {
				if (cid.contains(Integer.parseInt(key.toString())))
					iscc = true;
				else {
					tmpSet.clear();
					tmpSet.addAll(cc.getValue());
					tmpSet.retainAll(urate);
				}
				int i = cc.getKey() / Long.SIZE;
				int j = cc.getKey() % Long.SIZE;

				if (iscc || tmpSet.size() >= Constants.weakMark)
					mark[i] |= (1 << j);
			}

			boolean alone = true;
			for (long m : mark) {
				if (m != 0) {
					alone = false;
					break;
				}
			}
			if (alone) {
				for (int i = 0; i < mark.length; i++)
					mark[i] = 0xFFFFFFFFFFFFFFFFL;
			}

			StringBuffer out = new StringBuffer();
			out.append(Integer.toString(1));
			out.append(Constants.spliter);

			int offset = out.length();

			out.append(Long.toString(mark[0]));
			for (int i = 1; i < mark.length; i++) {
				out.append(Constants.userSpliter);
				out.append(Long.toString(mark[i]));
			}
			out.append(Constants.spliter);
			out.append(val);
			outVal.set(out.toString());
			context.write(key, outVal);
			outVal.set(out.substring(offset));
			if (iscc)
				mos.write(Constants.CenterPrefix, key, outVal);
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}

	/**
	 * the canopy file is created by saveCanopyCenter().
	 */
	public static void readCanopyCenter(BufferedReader br,
			HashMap<Integer, HashSet<Integer>> canopy) {
		try {
			StringSpliter sspliter = new StringSpliter();
			String t;
			while ((t = br.readLine()) != null) {
				sspliter.set(t, Constants.spliter);
				String mid = sspliter.next();
				if (mid == null)
					continue;
				HashSet<Integer> userID = new HashSet<Integer>();
				String uid;
				while ((uid = sspliter.next()) != null) {
					userID.add(Integer.parseInt(uid));
				}
				canopy.put(Integer.parseInt(mid), userID);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** save canopy center use format: id/u1/u2/u3... */
	private static void saveCanopyCenter(String path, FileSystem fs,
			HashMap<Integer, HashSet<Integer>> centers) {
		try {
			FSDataOutputStream fsds = fs.create(new Path(path));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsds));
			for (Entry<Integer, HashSet<Integer>> center : centers.entrySet()) {
				bw.append(Integer.toString(center.getKey()));
				for (Integer uid : center.getValue()) {
					bw.append(String.valueOf(Constants.spliter) + uid);
				}
				bw.newLine();
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * called by run(). merge reduce mos output canopy centers from in to out.
	 * NOTE: out canopy center id is NOT movie id
	 */
	private static int mergeCanopy(String in, String out, String cid,
			Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			HashMap<Integer, HashSet<Integer>> points = new HashMap<Integer, HashSet<Integer>>();
			HashMap<Integer, HashSet<Integer>> centers = new HashMap<Integer, HashSet<Integer>>();
			HashMap<Integer, HashSet<Integer>> outcc = new HashMap<Integer, HashSet<Integer>>();
			FileStatus[] files = fs.globStatus(new Path(in + "/"
					+ Constants.CanopyPrefix + "*"));
			for (FileStatus file : files) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(file.getPath())));
				readCanopyCenter(br, points);
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
					if (tmp.size() < Constants.strongerMark)
						left.put(point.getKey(), point.getValue());
				}
				HashMap<Integer, HashSet<Integer>> exchange = points;
				points = left;
				left = exchange;
			}

			ObjectOutputStream oos = new ObjectOutputStream(fs.create(new Path(
					cid)));
			HashSet<Integer> outset = new HashSet<Integer>();
			outset.addAll(centers.keySet());
			oos.writeObject(outset);
			oos.flush();
			oos.close();

			int index = 0;
			for (Entry<Integer, HashSet<Integer>> cc : centers.entrySet()) {
				outcc.put(index++, cc.getValue());
			}
			saveCanopyCenter(out, fs, outcc);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	/** no use */
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

				Long[] val = centerMarks.get(currentKey);
				if (tmp.size() >= 2) {
					val[center.getKey().intValue() >> 6] |= (1 << (center
							.getKey().intValue() & 63));
					val[currentKey.intValue() >> 6] |= (1 << (currentKey
							.intValue() & 63));
				}
			}
		}
	}

	/**
	 * args[0]:raw in ; args[1]:raw out ; args[2]:marked out ; args[3]:canopys.
	 */
	public static int run(String[] args, Configuration conf) {
		try {
			Job job = new Job(conf, "Canopy");
			job.setJarByClass(Canopy.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			String cpath = args[0] + "_canopys";
			job.getConfiguration().set(Constants.KMEANS_CANOPY_CENTERS, cpath);

			job.waitForCompletion(true);

			String ct = cpath + "/cid";

			if (mergeCanopy(cpath, args[3], ct, job.getConfiguration()) != 0)
				return -1;

			Job mark = new Job(conf, "mark");
			mark.setJarByClass(Canopy.class);
			mark.setMapperClass(MarkMap.class);
			mark.setOutputKeyClass(Text.class);
			mark.setOutputValueClass(Text.class);
			mark.setInputFormatClass(KeyValueTextInputFormat.class);
			FileInputFormat.addInputPath(mark, new Path(args[1]));
			FileOutputFormat.setOutputPath(mark, new Path(args[2]));

			MultipleOutputs.addNamedOutput(mark, Constants.CenterPrefix,
					TextOutputFormat.class, Text.class, Text.class);

			mark.getConfiguration().set(Constants.KMEANS_CANOPY_MOVIEID, ct);
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
