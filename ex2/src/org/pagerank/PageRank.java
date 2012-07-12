package org.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

;

public class PageRank {

	public static class Map extends Mapper<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] vals = value.toString().split("\\[");
			int outdegree = 1;
			for (int i = 1; i < vals.length; i++) {
				if (vals[i].startsWith("1"))
					outdegree++;
			}

			double PR = Double.parseDouble(vals[0]) / outdegree;
			for (int i = 1; i < vals.length; i++) {
				String[] v = vals[i].split("\\]");
				outKey.set(v[1]);
				if (v[0].startsWith("1")) {
					outVal.set(PR + "[0]" + key.toString());
				} else {
					outVal.set(0 + "[1]" + key.toString());
				}
				context.write(outKey, outVal);
			}
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public final static double beta = 0.85;

		private Text outVal = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double pr = 0;
			StringBuffer vals = new StringBuffer();
			for (Text val : value) {
				String[] v = val.toString().split("\\[");
				vals.append("[" + v[1]);
				pr += Double.parseDouble(v[0]);
			}
			pr = (1 - beta) + beta * pr;
			outVal.set(pr + vals.toString());
			context.write(key, outVal);
		}

	}

	public static class TransReduce extends
			Reducer<Text, Text, DoubleWritable, Text> {

		public final static double beta = 0.85;

		private DoubleWritable outKey = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double pr = 0;
			for (Text val : value) {
				String[] v = val.toString().split("\\[");
				pr += Double.parseDouble(v[0]);
			}
			pr = (1 - beta) + beta * pr;
			outKey.set(pr);
			context.write(outKey, key);
		}
	}

	public static Path run(String path, Configuration conf, int times) {
		// TODO Auto-generated method stub
		FileSystem fs;
		Path in = new Path(path);
		Path out = null;
		try {
			fs = FileSystem.get(conf);
			for (int i = 0; i < times; i++) {
				out = new Path(path
						+ i
						+ Integer.toString(new Random()
								.nextInt(Integer.MAX_VALUE)));
				Job job = new Job(conf, "page rank");
				job.setJarByClass(PageRank.class);
				job.setMapperClass(Map.class);
				job.setReducerClass(Reduce.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(KeyValueTextInputFormat.class);
				FileInputFormat.addInputPath(job, in);
				FileOutputFormat.setOutputPath(job, out);
				job.waitForCompletion(true);
				if (i > 0)
					fs.delete(in, true);
				in = out;
			}
			out = new Path(path + times
					+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
			Job transJob = new Job(conf, "page rank transfer");
			transJob.setJarByClass(PageRank.class);
			transJob.setMapperClass(Map.class);
			transJob.setReducerClass(TransReduce.class);
			transJob.setMapOutputKeyClass(Text.class);
			transJob.setMapOutputValueClass(Text.class);
			transJob.setOutputKeyClass(DoubleWritable.class);
			transJob.setOutputValueClass(Text.class);
			transJob.setInputFormatClass(KeyValueTextInputFormat.class);
			transJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(transJob, in);
			FileOutputFormat.setOutputPath(transJob, out);
			transJob.waitForCompletion(true);
			fs.delete(in, true);
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
		return out;
	}

	public static void main(String[] args) {
		boolean newGraph = false;
		boolean genGraph = false;
		int times = 1;
		List<String> leftArgs = new ArrayList<String>();
		String[] paths = new String[3];
		try {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			for (int i = 0; i < otherArgs.length; i++) {
				if (otherArgs[i].toLowerCase().equals("-newgraph")) {
					if (++i < otherArgs.length) {
						newGraph = true;
						paths[0] = otherArgs[i];
						continue;
					}
					leftArgs.clear();
					break;
				} else if (otherArgs[i].toLowerCase().equals("-gengraph")) {
					if (++i < otherArgs.length) {
						genGraph = true;
						paths[0] = otherArgs[i];
						continue;
					}
					leftArgs.clear();
					break;
				} else if (otherArgs[i].equals("-n")) {
					times = Integer.parseInt(otherArgs[++i]);
					continue;
				} else if (otherArgs[i].startsWith("-")) {
					continue;
				}
				leftArgs.add(otherArgs[i]);
			}

			if (newGraph || genGraph) {
				if ((genGraph && leftArgs.size() != 1)
						|| (newGraph && leftArgs.size() != 2)) {
					System.err
							.println("usage: pagerank [-newGraph|-genGraph <raw>] <in> [<out>]");
					System.exit(-2);
				}
				paths[1] = leftArgs.get(0);
				GraphGenerator.run(paths, conf);
				if (genGraph)
					System.exit(0);
			}
			if (leftArgs.size() != 2) {
				System.err
						.println("usage: pagerank [-newGraph|-genGraph <raw>] <in> <out>");
				System.exit(-3);
			}
			paths[1] = leftArgs.get(0);
			paths[2] = leftArgs.get(1);
			Path out = PageRank.run(paths[1], conf, times);
			PRSort.run(out, paths[2], conf);
			FileSystem.get(conf).delete(out, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
