package org.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class PRSort {

	public static class ReverseDoubleComparator extends WritableComparator {

		protected ReverseDoubleComparator() {
			super(DoubleWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			double thisValue = readDouble(b1, s1);
			double thatValue = readDouble(b2, s2);
			return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0
					: -1));
		}
	}

	public static void run(Path in, String out, Configuration conf) {
		try {
			Job job = new Job(conf, "PRSort");
			job.setJarByClass(PRSort.class);
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setPartitionerClass(TotalOrderPartitioner.class);
			job.setSortComparatorClass(ReverseDoubleComparator.class);
			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job, new Path(out));
			InputSampler.Sampler<DoubleWritable, Text> sampler = new InputSampler.RandomSampler<DoubleWritable, Text>(
					0.1, 10000, 10);
			in = in.makeQualified(in.getFileSystem(job.getConfiguration()));
			Path partitionFile = new Path(in.getParent(), "_Partitions");
			TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
					partitionFile);
			InputSampler
					.<DoubleWritable, Text> writePartitionFile(job, sampler);
			DistributedCache.addCacheFile(partitionFile.toUri(),
					job.getConfiguration());
			DistributedCache.createSymlink(job.getConfiguration());
			job.waitForCompletion(true);
			FileSystem.get(conf).delete(partitionFile, true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
