package org.kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopUsers {

	public static class Map extends Mapper<Text, Text, Text, IntWritable> {

		private Text outKey = new Text();
		private static final IntWritable outVal = new IntWritable(1);

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split(Canopy.spliter);
			for (String val : vals) {
				if (val.contains(Canopy.userSpliter)) {
					outKey.set(val.substring(0, val.indexOf(Canopy.userSpliter)));
					context.write(outKey, outVal);
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		IntWritable outVal = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : value) {
				sum += val.get();
			}
			outVal.set(sum);
			context.write(key, outVal);
		}
	}

	public static class SortMap extends Mapper<Text, Text, IntWritable, Text> {

		private IntWritable outKey = new IntWritable();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			outKey.set(Integer.parseInt(value.toString()));
			context.write(outKey, value);
		}
	}

	public static int run(String in, String out, String topfile,
			Configuration conf) {
		try {
			Job job = new Job(conf, "top users");
			job.setJarByClass(TopUsers.class);
			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			String countpath = out + "_count";

			FileInputFormat.addInputPath(job, new Path(in));
			FileOutputFormat.setOutputPath(job, new Path(countpath));

			job.waitForCompletion(true);

			Job sort = new Job(conf, "sort");
			sort.setJarByClass(TopUsers.class);
			sort.setMapperClass(SortMap.class);
			sort.setOutputKeyClass(IntWritable.class);
			sort.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(sort, new Path(countpath));
			FileOutputFormat.setOutputPath(sort, new Path(out));

			FileSystem fs = FileSystem.get(conf);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

}
