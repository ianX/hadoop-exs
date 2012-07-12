package org.pagerank;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
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

		Set<String> titles = new HashSet<String>();

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
				BufferedReader br = new BufferedReader(new FileReader(
						title.toString()));
				String t;
				while ((t = br.readLine()) != null) {
					this.titles.add(t);
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
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
				if (!val.equals("") && titles.contains(val.toString())) {
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

			FileStatus[] files = FileSystem.get(conf).globStatus(
					new Path(mid, "title*"));
			for (FileStatus file : files) {
				DistributedCache.addCacheFile(file.getPath().toUri(),
						filterJob.getConfiguration());
			}

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
