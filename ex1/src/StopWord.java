import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;

public class StopWord extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private String pattern = "\\p{Punct}";
		private Text outKey = new Text();
		private IntWritable outVal = new IntWritable();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String valTmp = value.toString().toLowerCase()
					.replaceAll("\'s", "");
			StringTokenizer tokens = new StringTokenizer(valTmp.replaceAll(
					pattern, " "));
			int total = 0;
			while (tokens.hasMoreTokens()) {
				total++;
				outKey.set(tokens.nextToken());
				outVal.set(1);
				output.collect(outKey, outVal);
			}
			outKey.set("!!total");
			outVal.set(total);
			output.collect(outKey, outVal);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		private JobConf job;

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			this.job = job;
			super.configure(job);
		}

		@Override
		public void reduce(Text key, Iterator<IntWritable> value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			int sum = 0;
			while (value.hasNext()) {
				sum += value.next().get();
			}
			if (key.toString().startsWith("!")) {
				job.setInt("wordcount.word.total", sum);
				return;
			}
			output.collect(key, new IntWritable(sum));
		}

	}

	public static class SortMap extends MapReduceBase implements
			Mapper<Text, Text, Text, NullWritable> {

		private int total = 1;
		private double threshold = 0.0005;

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			total = job.getInt("wordcount.word.total", 1);
			super.configure(job);
		}

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			if (Double.parseDouble(value.toString()) / total > threshold) {
				output.collect(key, NullWritable.get());
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length < 2 || args.length >3) {
			System.err.println("Usage: invertedindex <in> <out> [<stopword>]");
			System.exit(2);
		} else if (args.length == 2) {
			return 0;
		}
		JobConf wc = new JobConf(getConf(), StopWord.class);
		wc.setJobName("wordcount");
		wc.setMapperClass(Map.class);
		wc.setReducerClass(Reduce.class);
		wc.setOutputKeyClass(Text.class);
		wc.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(wc, new Path(args[0]));
		Path tmp = new Path(args[2] + "_tmp");
		FileOutputFormat.setOutputPath(wc, tmp);
		JobClient.runJob(wc);

		int total = wc.getInt("wordcount.word.total", 1000000);

		JobConf sw = new JobConf(getConf(), StopWord.class);
		sw.setJobName("stopword");
		sw.setInt("wordcount.word.total", total);
		sw.setOutputKeyClass(Text.class);
		sw.setOutputValueClass(NullWritable.class);
		sw.setMapperClass(SortMap.class);
		sw.setInputFormat(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.setInputPaths(sw, tmp);
		FileOutputFormat.setOutputPath(sw, new Path(args[2]));
		JobClient.runJob(sw);

		FileSystem.get(wc).delete(tmp, true);
		return 0;
	}

}
