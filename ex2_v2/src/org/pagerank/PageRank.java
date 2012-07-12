package org.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.pagerank.io.LinkWritable;
import org.pagerank.io.PRArrayWritable;
import org.pagerank.io.PRWritable;

public class PageRank {
	private final static String nullLink = "[[]]";

	public static class Map extends
			Mapper<Text, PRArrayWritable, Text, PRWritable> {
		private Text outKey = new Text();
		private LinkWritable outLink = new LinkWritable();
		private PRWritable outVal = new PRWritable();

		@Override
		public void map(Text key, PRArrayWritable value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double pr = value.getDouble() / value.getInt();
			Writable[] links = value.getLinkArray().get();
			for (Writable w : links) {
				LinkWritable link = (LinkWritable) w;

				if (link.getText().toString().equals(nullLink)) {
					outKey.set(key);
					outLink.set(false, nullLink);
					outVal.set(0, outLink);
				} else {
					outKey.set(link.getText());
					if (link.getBoolean()) {
						outLink.set(false, key.toString());
						outVal.set(pr, outLink);
					} else {
						outLink.set(true, key.toString());
						outVal.set(0, outLink);
					}
				}
				context.write(outKey, outVal);
			}
		}

	}

	public static class Reduce extends
			Reducer<Text, PRWritable, Text, PRArrayWritable> {

		public final static double beta = 0.85;

		private PRArrayWritable outVal = new PRArrayWritable();
		private List<LinkWritable> links = new ArrayList<LinkWritable>();

		@Override
		public void reduce(Text key, Iterable<PRWritable> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double pr = 0;
			int outdegree = 0;
			links.clear();
			for (PRWritable val : value) {
				links.add(new LinkWritable(val.getLink()));
				pr += val.getDouble();
				outdegree += val.getLink().getBoolean() ? 1 : 0;
			}
			pr = (1 - beta) + beta * pr;
			outVal.set(pr, outdegree,
					links.toArray(new LinkWritable[links.size()]));
			/*
			 * System.out.print(key.toString() + ">"); for (LinkWritable l :
			 * links) { System.out.print(l.getText().toString() + ","); }
			 * System.out.println();
			 */
			context.write(key, outVal);
		}

	}

	/*
	 * public static class TransMap extends Mapper<Text, PRArrayWritable,
	 * DoubleWritable, Text> {
	 * 
	 * @Override public void map(Text key, PRArrayWritable value, Context
	 * context) throws IOException, InterruptedException {
	 * context.write(value.getDoubleWritable(), key); } }
	 */

	public static class TransReduce extends
			Reducer<Text, PRWritable, DoubleWritable, Text> {

		public final static double beta = 0.85;

		private DoubleWritable outKey = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<PRWritable> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double pr = 0;
			for (PRWritable val : value) {
				pr += val.getDouble();
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
				job.setMapOutputValueClass(PRWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(PRArrayWritable.class);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
				SequenceFileInputFormat.addInputPath(job, in);
				SequenceFileOutputFormat.setOutputPath(job, out);
				SequenceFileOutputFormat.setCompressOutput(job, true);
				SequenceFileOutputFormat.setOutputCompressionType(job,
						SequenceFile.CompressionType.BLOCK);
				job.waitForCompletion(true);
				if (i > 0)
					fs.delete(in, true);
				in = out;
			}
			out = new Path(path + times
					+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
			Job transJob = new Job(conf, "page rank transfer");
			transJob.setJarByClass(PageRank.class);
			// transJob.setMapperClass(TransMap.class);
			transJob.setMapperClass(Map.class);
			transJob.setReducerClass(TransReduce.class);
			transJob.setMapOutputKeyClass(Text.class);
			transJob.setMapOutputValueClass(PRWritable.class);
			transJob.setOutputKeyClass(DoubleWritable.class);
			transJob.setOutputValueClass(Text.class);
			transJob.setInputFormatClass(SequenceFileInputFormat.class);
			transJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileInputFormat.addInputPath(transJob, in);
			SequenceFileOutputFormat.setOutputPath(transJob, out);
			SequenceFileOutputFormat.setCompressOutput(transJob, true);
			SequenceFileOutputFormat.setOutputCompressionType(transJob,
					SequenceFile.CompressionType.BLOCK);
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
