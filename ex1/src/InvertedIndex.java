import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {

	public static class MyInputFormat extends TextInputFormat {
		@Override
		protected boolean isSplitable(FileSystem fs, Path filename) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public RecordReader<LongWritable, Text> getRecordReader(
				InputSplit genericSplit, JobConf job, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			reporter.setStatus(genericSplit.toString());
			return new MyRecordReader(job, (FileSplit) genericSplit);
		}
	}

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private boolean useStopword = false;
		private Set<String> stopword = new HashSet<String>();
		private String fname = null;

		public void configure(JobConf job) {
			fname = job.get("map.input.file");
			fname = fname.substring(fname.lastIndexOf('/') + 1);
			if (useStopword = job.getBoolean("invertedindex.use.stopword",
					false)) {
				Path[] stopwordFiles = new Path[0];
				try {
					stopwordFiles = DistributedCache.getLocalCacheFiles(job);
					for (Path stopwordFile : stopwordFiles) {
						parseStopwordFile(stopwordFile);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		private void parseStopwordFile(Path path) throws IOException {
			BufferedReader fis = new BufferedReader(new FileReader(
					path.toString()));
			String word = null;
			while ((word = fis.readLine()) != null) {
				stopword.add(word);
			}
		}

		// private String pattern =
		// "!|\"|#|$|%|&|(|)|\\*|\\+|,|-|\\.|/|:|;|<|=|>|\\?|@|[|\\|]|\\^|_|`|\\{|\\|\\}|~|\'s";
		private String pattern = "\\p{Punct}";
		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String valTmp = value.toString().toLowerCase()
					.replaceAll("\'s", "");
			StringTokenizer tokens = new StringTokenizer(valTmp.replaceAll(
					pattern, " "));
			int pos = 0;
			while (tokens.hasMoreTokens()) {
				String token = tokens.nextToken();
				if (useStopword && stopword.contains(token)) {
					pos++;
					continue;
				}
				outKey.set(token + ":" + fname);
				outVal.set("1" + ":(" + key.get() + "," + (pos++) + ")");
				output.collect(outKey, outVal);
			}
		}
	}

	public static class Combine extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] keys = key.toString().split(":");
			int sum = 0;
			StringBuffer pos = new StringBuffer();
			while (values.hasNext()) {
				String[] vals = values.next().toString().split(":");
				sum += Integer.parseInt(vals[0].toString());
				pos.append(":" + vals[1]);
			}
			outKey.set(keys[0]);
			outVal.set(keys[1] + "[" + sum + "]" + pos);
			output.collect(outKey, outVal);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer sb = new StringBuffer();
			if (values.hasNext()) {
				sb.append(" -> " + values.next().toString());
			}
			while (values.hasNext()) {
				sb.append(" , " + values.next().toString());
			}
			output.collect(key, new Text(sb.toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(getConf(), InvertedIndex.class);
		if (args.length == 3) {
			FileStatus[] filestatus = FileSystem.get(conf).globStatus(
					new Path(args[2] + "/p*"));
			for (int i = 0; i < filestatus.length; i++) {
				DistributedCache.addCacheFile(filestatus[i].getPath().toUri(),
						conf);
			}
			conf.setBoolean("invertedindex.use.stopword", true);
		}
		conf.setJobName("inverted index");
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(MyInputFormat.class);
		MyInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StopWord(), args);
		if (res != 0)
			System.exit(res);
		res = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
		System.exit(res);
	}
}
