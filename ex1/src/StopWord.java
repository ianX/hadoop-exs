import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
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
		private String total_path;

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			this.job = job;
			total_path = job.get("wordcount.total.path", null);
			super.configure(job);
		}

		private void saveTotal(int total) {
			if (total_path != null) {
				try {
					FSDataOutputStream os = FileSystem.get(job).create(
							new Path(total_path));
					BufferedWriter bw = new BufferedWriter(
							new OutputStreamWriter(os.getWrappedStream()));
					bw.write(Integer.toString(total));
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
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
				saveTotal(sum);
				return;
			}
			output.collect(key, new IntWritable(sum));
		}

	}

	public static class SortMap extends MapReduceBase implements
			Mapper<Text, Text, Text, NullWritable> {

		private int total = 1;
		private double threshold = 0.001;

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
		List<String> other_args = new ArrayList<String>();
		for (String s : args) {
			if (s.contains("-"))
				continue;
			other_args.add(s);
		}

		if (other_args.size() < 2 || other_args.size() > 3) {
			System.err
					.println("Usage: invertedindex <in> <out> [<stopword>] [command]");
			System.exit(-1);
		} else if (other_args.size() == 2) {
			return 0;
		}
		JobConf wc = new JobConf(getConf(), StopWord.class);
		if (FileSystem.get(wc).exists(new Path(other_args.get(2)))) {
			return 0;
		}
		wc.setJobName("wordcount");
		wc.setMapperClass(Map.class);
		wc.setReducerClass(Reduce.class);
		wc.setOutputKeyClass(Text.class);
		wc.setOutputValueClass(IntWritable.class);
		wc.setOutputFormat(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(wc, new Path(other_args.get(0)));
		Path tmp = new Path(other_args.get(2) + "_tmp_"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		SequenceFileOutputFormat.setOutputPath(wc, tmp);
		Path totalPath = new Path(other_args.get(2) + "_total_tmp_"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		wc.set("wordcount.total.path", totalPath.toString());

		JobClient.runJob(wc);

		FileSystem fs = FileSystem.get(wc);
		fs.deleteOnExit(totalPath);

		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(totalPath)));

		int total = Integer.parseInt(br.readLine());
		br.close();

		JobConf sw = new JobConf(getConf(), StopWord.class);
		sw.setJobName("stopword");
		sw.setInt("wordcount.word.total", total);
		sw.setOutputKeyClass(Text.class);
		sw.setOutputValueClass(NullWritable.class);
		sw.setMapperClass(SortMap.class);
		sw.setInputFormat(SequenceFileAsTextInputFormat.class);
		SequenceFileAsTextInputFormat.setInputPaths(sw, tmp);
		FileOutputFormat.setOutputPath(sw, new Path(other_args.get(2)));
		JobClient.runJob(sw);

		FileSystem.get(wc).delete(tmp, true);
		return 0;
	}

}
