package org.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Kmeans {

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
			String[] markString = vals[0].split(Canopy.userSpliter);
			long[] marks = new long[markString.length];
			for (int i = 0; i < markString.length; i++) {
				marks[i] = Long.parseLong(markString[i]);
			}

			for (int i = 1; i < vals.length; i++) {
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
			outVal.set(key.toString() + Canopy.spliter + value.toString());
			context.write(outKey, outVal);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private HashMap<Integer, HashSet<Integer>> canopy = new HashMap<Integer, HashSet<Integer>>();

		public void setup(Context context) throws IOException,
				InterruptedException {
			Path[] canopys = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path cc : canopys) {
				Canopy.readCanopy(cc, this.canopy);
			}
			context.setStatus("Kmeans Reduce: read canopy success");
		}

		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
		}
	}

	public static void main(String[] args) {

	}
}
