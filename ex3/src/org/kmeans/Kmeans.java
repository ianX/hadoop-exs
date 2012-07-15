package org.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.util.MapComprator;
import org.util.MinHeap;

public class Kmeans {

	public static final String mrSpliter = ":";

	public static class Map extends Mapper<Text, Text, Text, Text> {
		private HashMap<Integer, HashMap<Integer, Integer>> centers = new HashMap<Integer, HashMap<Integer, Integer>>();
		private HashMap<Integer, Long[]> centerMarks = new HashMap<Integer, Long[]>();
		private Set<Integer> tmpSet = new HashSet<Integer>();

		public void setup(Context context) throws IOException,
				InterruptedException {
			String cp = context.getConfiguration().get("kmeans.centers.file",
					"");
			if (cp.length() > 0) {
				readCenters(cp, centers, centerMarks, context);
			}
			context.setStatus("Kmeans Map: read centers success");
		}

		private void readCenters(String path,
				HashMap<Integer, HashMap<Integer, Integer>> centers,
				HashMap<Integer, Long[]> centerMarks, Context context) {
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						FileSystem.get(context.getConfiguration()).open(
								new Path(path))));
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
						HashMap<Integer, Integer> rate = new HashMap<Integer, Integer>();
						for (int i = 1; i < vals.length; i++) {
							String[] r = vals[i].split(Canopy.userSpliter);
							if (r.length == 2)
								rate.put(Integer.parseInt(r[0]),
										Integer.parseInt(r[1]));
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
				HashMap<Integer, Integer> cval = centers.get(cid);
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
				Canopy.readCanopy(cc, this.canopy);
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
			for (Entry<Integer, HashSet<Integer>> center : this.canopy
					.entrySet()) {
				tmpSet.clear();
				tmpSet.addAll(center.getValue());
				tmpSet.retainAll(newCenter.keySet());
				int i = center.getKey() / Long.SIZE;
				int j = center.getKey() % Long.SIZE;
				mark[i] &= (tmpSet.size() >= Canopy.weakMark ? (1 << j)
						: (0 << j));
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
			mos.write("center", key, outCenter);
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}

	public static int run(String[] args, Configuration conf) {
		return 0;
	}

	public static void main(String[] args) {

	}
}
