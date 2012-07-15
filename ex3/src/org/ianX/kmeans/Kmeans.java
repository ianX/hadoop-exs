package org.ianX.kmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.ianX.util.MapComprator;
import org.ianX.util.MinHeap;

public class Kmeans {

	public static final String CenterPrefix = "center";
	public static final String mrSpliter = ":";
	public static final String KMEANS_CENTERS_FILE = "kmeans.centers.file";

	public static class Map extends Mapper<Text, Text, Text, Text> {
		private HashMap<Integer, HashMap<Integer, Double>> centers = new HashMap<Integer, HashMap<Integer, Double>>();
		private HashMap<Integer, Long[]> centerMarks = new HashMap<Integer, Long[]>();
		private Set<Integer> tmpSet = new HashSet<Integer>();

		public void setup(Context context) throws IOException,
				InterruptedException {
			String cp = context.getConfiguration().get(KMEANS_CENTERS_FILE, "");
			if (cp.length() > 0) {
				FileStatus[] files = FileSystem.get(context.getConfiguration())
						.globStatus(new Path(cp));
				for (FileStatus file : files) {
					readCenters(file.getPath(), centers, centerMarks, context);
				}
			}
			context.setStatus("Kmeans Map: read centers success");
		}

		private void readCenters(Path path,
				HashMap<Integer, HashMap<Integer, Double>> centers,
				HashMap<Integer, Long[]> centerMarks, Context context) {
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						FileSystem.get(context.getConfiguration()).open(path)));
				String line;
				while ((line = br.readLine()) != null) {
					String[] words = line.split("\t");
					if (words.length == 2) {
						int key = Integer.parseInt(words[0]);
						String[] vals = words[1].split(Canopy.spliter);
						if (vals.length != 0) {
							String[] markString = vals[0]
									.split(Canopy.userSpliter);
							Long[] marks = new Long[markString.length];
							for (int i = 0; i < markString.length; i++) {
								marks[i] = Long.parseLong(markString[i]);
							}
							centerMarks.put(key, marks);
						}
						HashMap<Integer, Double> rate = new HashMap<Integer, Double>();
						for (int i = 1; i < vals.length; i++) {
							String[] r = vals[i].split(Canopy.userSpliter);
							if (r.length == 2)
								rate.put(Integer.parseInt(r[0]),
										Double.parseDouble(r[1]));
						}
						centers.put(key, rate);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private Text outKey = new Text();
		private Text outVal = new Text();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			HashMap<Integer, Integer> urate = new HashMap<Integer, Integer>();
			String val = value.toString();
			String[] vals = val.split(Canopy.spliter);

			// vals[0] is center, just ignore in map
			String[] markString = vals[1].split(Canopy.userSpliter);
			long[] marks = new long[markString.length];
			for (int i = 0; i < markString.length; i++) {
				marks[i] = Long.parseLong(markString[i]);
			}
			for (int i = 2; i < vals.length; i++) {
				String[] r = vals[i].split(Canopy.userSpliter);
				if (r.length == 2)
					urate.put(Integer.parseInt(r[0]), Integer.parseInt(r[1]));
			}

			Integer ckey = 0;
			double mindis = Double.MAX_VALUE;
			double dis = 0;
			double cross = 0;
			double ulen = 0;
			double clen = 0;
			for (Integer cid : centers.keySet()) {
				Long[] cmark = centerMarks.get(cid);
				boolean inf = true;
				if (cmark.length == marks.length) {
					for (int i = 0; i < marks.length; i++) {
						if ((cmark[i].longValue() & marks[i]) != 0) {
							inf = false;
							break;
						}
					}
				}
				if (inf)
					continue;
				HashMap<Integer, Double> cval = centers.get(cid);
				tmpSet.clear();
				tmpSet.addAll(urate.keySet());
				tmpSet.retainAll(cval.keySet());
				cross = 0;
				ulen = 0;
				clen = 0;
				for (Integer rate : tmpSet) {
					double c = cval.get(rate).doubleValue();
					double u = urate.get(rate).doubleValue();
					cross += c * u;
					ulen += u * u;
					clen += c * c;
				}
				ulen = ulen > 0 ? Math.sqrt(ulen) : 1;
				clen = clen > 0 ? Math.sqrt(clen) : 1;
				dis = cross / (ulen * clen);
				if (dis < mindis)
					ckey = cid;
			}
			outKey.set(ckey.toString());
			outVal.set(key.toString() + mrSpliter + value.toString());
			context.write(outKey, outVal);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private HashMap<Integer, HashSet<Integer>> canopy = new HashMap<Integer, HashSet<Integer>>();
		private HashMap<Integer, Integer> counts = new HashMap<Integer, Integer>();
		private HashMap<Integer, Double> sums = new HashMap<Integer, Double>();
		private MapComprator<Integer, Integer> mc = new MapComprator<Integer, Integer>(
				counts);
		private MinHeap<Integer> min = new MinHeap<Integer>(mc);

		private HashMap<Integer, Double> newCenter = new HashMap<Integer, Double>();
		private HashSet<Integer> tmpSet = new HashSet<Integer>();

		@SuppressWarnings("rawtypes")
		private MultipleOutputs mos;

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs(context);
			Path[] canopys = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path cc : canopys) {
				BufferedReader br = new BufferedReader(new FileReader(
						cc.toString()));
				Canopy.readCanopy(br, this.canopy);
			}
			context.setStatus("Kmeans Reduce: read canopy success");
		}

		private Text outKey = new Text();
		private Text outVal = new Text();

		private Text outCenter = new Text();

		@SuppressWarnings("unchecked")
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			for (Text val : value) {
				String[] vals = val.toString().split(mrSpliter);
				if (vals.length == 2) {
					outKey.set(vals[0]);
					outVal.set(vals[1].replace("[0-9]", key.toString()));
					context.write(outKey, outVal);
					String[] users = vals[1].split(Canopy.spliter);

					// users[0] & users[1] are center & canopy, just ignore in
					// reduce
					for (int i = 2; i < users.length; i++) {
						String[] u = users[i].split(Canopy.userSpliter);
						if (u.length == 2) {
							Integer k = Integer.parseInt(u[0]);
							Double v = Double.parseDouble(u[1]);
							if (counts.containsKey(k)) {
								counts.put(k, counts.get(k) + 1);
								sums.put(k, sums.get(k) + v);
							} else {
								counts.put(k, 1);
								sums.put(k, v);
							}
						}
					}
				}
			}

			Iterator<Integer> itr = counts.keySet().iterator();

			min.ensureCapacity(1000);
			int size = 0;
			while (itr.hasNext() && size < 1000) {
				min.push(itr.next());
				size++;
			}

			while (itr.hasNext()) {
				min.change(itr.next());
			}

			Integer uid;
			while ((uid = min.pop()) != null) {
				newCenter.put(uid, sums.get(uid) / counts.get(uid));
			}
			long[] mark = new long[this.canopy.size() / Long.SIZE + 1];
			for (Entry<Integer, HashSet<Integer>> center : canopy.entrySet()) {
				tmpSet.clear();
				tmpSet.addAll(center.getValue());
				tmpSet.retainAll(newCenter.keySet());
				int i = center.getKey() / Long.SIZE;
				int j = center.getKey() % Long.SIZE;
				
				if (tmpSet.size() >= Canopy.weakMark)
					mark[i] |= (1 << j);
			}
			StringBuffer sb = new StringBuffer();
			sb.append(Long.toString(mark[0]));
			for (int i = 1; i < mark.length; i++) {
				sb.append(Canopy.userSpliter);
				sb.append(Long.toString(mark[i]));
			}
			for (Entry<Integer, Double> user : newCenter.entrySet()) {
				sb.append(Canopy.spliter);
				sb.append(user.getKey().toString());
				sb.append(Canopy.userSpliter);
				sb.append(user.getValue().toString());
			}
			outCenter.set(sb.toString());
			mos.write(CenterPrefix, key, outCenter);
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}

	/** args[0]:canopy; args[1]:init center; args[2]:marked data; args[3]:outdir */
	public static int run(String[] args, Configuration conf, int n) {
		try {
			FileSystem fs = FileSystem.get(conf);
			String canopy = args[0];
			String center = args[1];
			String in = args[2];
			String out = "";
			for (int i = 0; i < n; i++) {
				if (i == n - 1)
					out = args[3];
				else
					out = args[3] + "_" + i;
				Job job = new Job(conf, "kmeans");
				job.setJarByClass(Kmeans.class);
				job.setMapperClass(Map.class);
				job.setReducerClass(Reduce.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(KeyValueTextInputFormat.class);

				job.getConfiguration().set(KMEANS_CENTERS_FILE, center);
				FileStatus[] canopyFiles = fs.globStatus(new Path(canopy));
				for (FileStatus cf : canopyFiles) {
					DistributedCache.addCacheFile(cf.getPath().toUri(),
							job.getConfiguration());
				}

				FileInputFormat.addInputPath(job, new Path(in));
				FileOutputFormat.setOutputPath(job, new Path(out));
				MultipleOutputs.addNamedOutput(job, CenterPrefix,
						TextOutputFormat.class, Text.class, Text.class);
				job.waitForCompletion(true);

				center = out + "/" + CenterPrefix + "*";
				in = out + "/part*";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	/** args[0]:raw in; args[1]:marked out; args[2]:canopy path */
	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();

			boolean raw = false;

			ArrayList<String> params = new ArrayList<String>();
			for (String s : otherArgs) {
				if (s.equals("-new")) {
					raw = true;
					continue;
				}
				params.add(s);
			}

			if (params.size() != 3) {
				System.err
						.println("usage: kmeans [-new] inDir outDir canopyDir");
				System.exit(-1);
			}

			String raw_in = params.get(0);
			String raw_out = params.get(0) + "_raw";
			String marked = params.get(0) + "_marked";
			String canopyPath = params.get(2);
			String canopy = params.get(2) + "/" + "canopys";
			String init_center = marked + "/" + CenterPrefix;
			String out = params.get(1);

			String[] paths = new String[4];

			FileSystem fs = FileSystem.get(conf);

			if (raw) {
				Path cp = new Path(canopyPath);
				if (!fs.exists(cp))
					fs.delete(cp, true);
				fs.mkdirs(new Path(canopyPath));
				marked = params.get(0) + "_marked";
				paths[0] = raw_in;
				paths[1] = raw_out;
				paths[2] = marked;
				paths[3] = canopy;
				Canopy.run(paths, conf);
			} else {
				Path cy = new Path(canopy);
				if (!fs.exists(cy) || !fs.isFile(cy)) {
					System.err.println("canopy file error");
					System.exit(-1);
				}
				marked = params.get(0) + "/part*";
			}
			init_center = marked + "/" + CenterPrefix + "*";

			paths[0] = canopy;
			paths[1] = init_center;
			paths[2] = marked + "/part*";
			paths[3] = out;
			Kmeans.run(paths, conf, 3);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
