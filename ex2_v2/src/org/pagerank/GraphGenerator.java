package org.pagerank;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.pagerank.io.LinkWritable;
import org.pagerank.io.PRArrayWritable;

public class GraphGenerator {

	private final static String nullLink = "[[]]";

	public static class Map extends
			Mapper<LongWritable, Text, Text, LinkWritable> {

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
		private static final Pattern plink = Pattern
				.compile("\\[\\[([\\p{Print}&&[^\\]]]*)\\]\\]");

		private Text outKey = new Text();
		private LinkWritable outVal = new LinkWritable();

		@SuppressWarnings("unchecked")
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String val = value.toString();
			Matcher mtitle = ptitle.matcher(val);
			if (mtitle.find()) {
				String title = mtitle.group(1);
				Matcher mlink = plink.matcher(val);
				String[] links;
				outKey.set(title);
				// System.out.print(outKey.toString() + ">");
				while (mlink.find()) {
					links = mlink.group(1).split("\\|");
					// if (!links[0].contains(":")) {
					if (links.length > 0) {
						outVal.set(true, links[0]);
						context.write(outKey, outVal);
					}
					// System.out.print(outVal.getText().toString() + ",");
					// }
				}
				// System.out.println();
				outVal.set(false, nullLink);
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

	public static class Reduce extends
			Reducer<Text, LinkWritable, Text, PRArrayWritable> {

		private List<LinkWritable> vals = new ArrayList<LinkWritable>();
		private PRArrayWritable outVal = new PRArrayWritable();

		@Override
		public void reduce(Text key, Iterable<LinkWritable> value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			vals.clear();
			int outdegree = 0;
			// System.out.print(key.toString() + ">");
			for (LinkWritable val : value) {
				outdegree += val.getBoolean() ? 1 : 0;
				vals.add(new LinkWritable(val));
				// System.out.print(val.getText().toString() + ",");
			}
			outVal.set(1, outdegree,
					vals.toArray(new LinkWritable[vals.size()]));
			context.write(key, outVal);
		}
	}

	public static class FilterMap extends
			Mapper<Text, PRArrayWritable, Text, LinkWritable> {

		Set<String> titles = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			Path[] titles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path title : titles) {
				readTitle(title);
			}
			this.titles.add(nullLink);
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
		LinkWritable outVal = new LinkWritable();

		@Override
		public void map(Text key, PRArrayWritable value, Context context)
				throws IOException, InterruptedException {
			Writable[] vals = value.getLinkArray().get();
			// System.out.print(key.toString() + ">");
			for (Writable val : vals) {
				LinkWritable v = (LinkWritable) val;
				if (titles.contains(v.getText().toString())) {
					context.write(key, v);
					// System.out.print(v.getText().toString() + ",");
					if (!v.getText().equals(nullLink)) {
						outKey.set(v.getText());
						outVal.set(false, key.toString());
						context.write(outKey, outVal);
					}
				}
			}
			// System.out.println();
		}
	}

	public static void run(String[] args, Configuration conf) {
		try {
			Job job = new Job(conf, "page rank graph generator");
			job.setJarByClass(GraphGenerator.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LinkWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(PRArrayWritable.class);

			Path mid = new Path(args[1] + "_"
					+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
			FileInputFormat.addInputPath(job, new Path(args[0]));
			SequenceFileOutputFormat.setOutputPath(job, mid);

			MultipleOutputs.addNamedOutput(job, "title",
					TextOutputFormat.class, Text.class, NullWritable.class);
			job.waitForCompletion(true);

			Job filterJob = new Job(conf, "page rank fliter");
			filterJob.setJarByClass(GraphGenerator.class);
			filterJob.setMapperClass(FilterMap.class);
			filterJob.setReducerClass(Reduce.class);
			filterJob.setMapOutputKeyClass(Text.class);
			filterJob.setMapOutputValueClass(LinkWritable.class);
			filterJob.setOutputKeyClass(Text.class);
			filterJob.setOutputValueClass(PRArrayWritable.class);
			filterJob.setInputFormatClass(SequenceFileInputFormat.class);
			filterJob.setOutputFormatClass(SequenceFileOutputFormat.class);

			SequenceFileInputFormat.addInputPath(filterJob, new Path(mid,
					"part*"));
			SequenceFileOutputFormat
					.setOutputPath(filterJob, new Path(args[1]));
			SequenceFileOutputFormat.setCompressOutput(filterJob, true);
			SequenceFileOutputFormat.setOutputCompressionType(filterJob,
					SequenceFile.CompressionType.BLOCK);

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
