package org.pagerank;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GraphGenerator {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@SuppressWarnings("rawtypes")
		private MultipleOutputs mos;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs(context);
		}

		private static final Pattern ptitle = Pattern
				.compile("&lttitle&gt(.*)&lt/title&gt");
		// private static final Pattern plink = Pattern
		// .compile("\\[\\[([^\\]\t]*)\\]\\]");

		private Text outKey = new Text();
		private Text outVal = new Text();

		@SuppressWarnings("unchecked")
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String val = value.toString();
			Matcher mtitle = ptitle.matcher(val);
			if (mtitle.find()) {
				String title = mtitle.group(1);
				// Matcher mlink = plink.matcher(val);
				String[] links = val.split("\\[\\[");
				StringBuffer vals = new StringBuffer();
				outKey.set(title);
				for (int i = 1; i < links.length; i++) {
					String[] link = links[i].split("\\]\\]")[0].split("\\|");
					if (link.length > 0) {
						vals.append(link[0] + "[");
					}
				}

				outVal.set(vals.toString());
				context.write(outKey, outVal);
				mos.write("title", outKey, NullWritable.get());
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}

	public static class FilterMap extends Mapper<Text, Text, Text, Text> {

		BitSet bits = new BitSet(bitSize);

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			Path[] titles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path title : titles) {
				readTitle(title);
			}
		}

		private void readTitle(Path title) {
			try {
				ObjectInputStream ois = new ObjectInputStream(
						new FileInputStream(title.toString()));
				bits = (BitSet) ois.readObject();
				ois.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		Text outKey = new Text();
		Text outVal = new Text();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split("\\[");
			for (String val : vals) {
				if (!val.equals("") && exist(bits, val)) {
					outVal.set("1]" + val);
					context.write(key, outVal);

					outKey.set(val);
					outVal.set("0]" + key.toString());
					context.write(outKey, outVal);
				}
			}
		}
	}

	public static class FilterReduce extends Reducer<Text, Text, Text, Text> {

		private Text outVal = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuffer vals = new StringBuffer();
			vals.append("1");
			for (Text link : value) {
				vals.append("[" + link.toString());
			}
			outVal.set(vals.toString());
			context.write(key, outVal);
		}
	}

	private static final int bitSize = 1 << 28;
	private static final int basic = bitSize - 1;

	private static int[] lrandom(String key) {
		int[] randomsum = new int[4];
		int random0 = hashCode(key, 1);
		int random1 = hashCode(key, 2);
		int random2 = hashCode(key, 5);
		int random3 = hashCode(key, 7);
		randomsum[0] = random0;
		randomsum[1] = random1;
		randomsum[2] = random2;
		randomsum[3] = random3;
		return randomsum;
	}

	private static void add(BitSet bits, String key) {
		if (exist(bits, key)) {
			return;
		}
		int keyCode[] = lrandom(key);
		bits.set(keyCode[0]);
		bits.set(keyCode[1]);
		bits.set(keyCode[2]);
		bits.set(keyCode[3]);
	}

	private static boolean exist(BitSet bits, String key) {
		int keyCode[] = lrandom(key);
		if (bits.get(keyCode[0]) && bits.get(keyCode[1])
				&& bits.get(keyCode[2]) && bits.get(keyCode[3])) {
			return true;
		}
		return false;
	}

	private static int hashCode(String key, int Q) {
		int h = 0;
		char val[] = key.toCharArray();
		int len = key.length();
		for (int i = 0; i < len; i++) {
			h = (26 + 2 * Q + 1) * h + (val[i] * Q << 11) + val[i] + (h >> 19);
		}
		return (basic & h);
	}

	public static void run(String[] args, Configuration conf) {
		try {
			Job job = new Job(conf, "page rank graph generator");
			job.setJarByClass(GraphGenerator.class);
			job.setMapperClass(Map.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Path mid = new Path(args[1] + "_"
					+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, mid);

			MultipleOutputs.addNamedOutput(job, "title",
					TextOutputFormat.class, Text.class, NullWritable.class);
			job.waitForCompletion(true);

			Job filterJob = new Job(conf, "page rank fliter");
			filterJob.setJarByClass(GraphGenerator.class);
			filterJob.setMapperClass(FilterMap.class);
			filterJob.setReducerClass(FilterReduce.class);
			filterJob.setMapOutputKeyClass(Text.class);
			filterJob.setMapOutputValueClass(Text.class);
			filterJob.setOutputKeyClass(Text.class);
			filterJob.setOutputValueClass(Text.class);
			filterJob.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.addInputPath(filterJob, new Path(mid, "part*"));
			FileOutputFormat.setOutputPath(filterJob, new Path(args[1]));

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] files = fs.globStatus(new Path(mid, "title*"));

			BitSet bits = new BitSet(bitSize);
			for (FileStatus file : files) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(file.getPath())));
				String line;
				while ((line = br.readLine()) != null) {
					add(bits, line);
				}
				br.close();
			}

			Path filterPath = new Path(mid, "filter");

			ObjectOutputStream oos = new ObjectOutputStream(
					fs.create(filterPath));
			oos.writeObject(bits);
			oos.flush();
			oos.close();

			DistributedCache.addCacheFile(filterPath.toUri(),
					filterJob.getConfiguration());

			filterJob.waitForCompletion(true);

			// FileSystem.get(conf).delete(mid, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
