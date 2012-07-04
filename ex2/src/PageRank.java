import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author ian
 * 
 */
public class PageRank extends Configured implements Tool {
	public static String spliter = ":";

	public static class GraphMap extends MapReduceBase implements
			Mapper<Object, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void map(Object key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String val = value.toString().trim();
			Pattern pt = Pattern.compile("&lttitle&gt([A-Za-z0-9 ]*)&lt/title&gt");
			Pattern pl = Pattern.compile("\\[\\[([A-Za-z0-9 ]*)\\]\\]");
			Matcher mt = pt.matcher(val);
			Matcher ml = pl.matcher(val);
			if (!mt.find())
				return;
			outKey.set(mt.group(1));
			StringBuffer links = new StringBuffer("1");
			while (ml.find()) {
				links.append(":" + ml.group(1));
			}
			outVal.set(links.toString());
			output.collect(outKey, value);
		}

	}

	public static class Map extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String[] vals = value.toString().split(spliter);
			String pr = Double.toString(Double.parseDouble(vals[0])
					/ vals.length);
			output.collect(key, new Text("!"));
			output.collect(key, value);
			for (int i = 1; i < vals.length; i++) {
				outKey.set(vals[i]);
				outVal.set(pr);
				output.collect(outKey, outVal);
			}
		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public final static double beta = 0.85;

		private Text outVal = new Text();

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			boolean isKeyExist = false;
			String links = null;
			double pr = 0;
			while (value.hasNext()) {
				String val = value.next().toString();
				if (val.startsWith("!")) {
					isKeyExist = true;
					continue;
				}
				if (val.contains(spliter)) {
					links = val.substring(val.indexOf(spliter));
					continue;
				}
				pr += Double.parseDouble(val);
			}
			if (!isKeyExist)
				return;
			pr = (1 - beta) + beta * pr;
			if (links != null)
				outVal.set(pr + links);
			else
				outVal.set(Double.toString(pr));
			output.collect(key, outVal);
		}

	}

	public static class SortMap extends MapReduceBase implements
			Mapper<Text, Text, DoubleWritable, Text> {

		private DoubleWritable outKey = new DoubleWritable();

		@Override
		public void map(Text key, Text value,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String val = value.toString();
			outKey.set(Double.parseDouble(val.substring(0, val.indexOf(spliter))));
			output.collect(outKey, key);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.err.println("Usage: pagerank <in> <out>");
			System.exit(2);
		}

		Vector<Path> tmpDirs = new Vector<Path>();
		JobConf gen = new JobConf(getConf(), PageRank.class);
		tmpDirs.add(new Path(args[0] + "_"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE))));
		gen.setJobName("graphgenerator");
		gen.setMapperClass(GraphMap.class);
		gen.setOutputKeyClass(Text.class);
		gen.setOutputValueClass(Text.class);
		gen.setInputFormat(TextInputFormat.class);
		gen.setOutputFormat(SequenceFileOutputFormat.class);
		TextInputFormat.setInputPaths(gen, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(gen, tmpDirs.lastElement());
		JobClient.runJob(gen);
		FileSystem.get(gen).deleteOnExit(tmpDirs.lastElement());
		
		int times = 3;
		JobConf conf;
		for (int i = 0; i < times; i++) {
			conf = new JobConf(getConf(), PageRank.class);
			conf.setJobName("pagerank" + i);
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setInputFormat(SequenceFileAsTextInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			SequenceFileAsTextInputFormat.setInputPaths(conf, tmpDirs.lastElement());
			tmpDirs.add(new Path(args[1] + "_"
					+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE))));
			SequenceFileOutputFormat.setOutputPath(conf, tmpDirs.lastElement());
			JobClient.runJob(conf);
			FileSystem.get(conf).deleteOnExit(tmpDirs.lastElement());
		}
		
		JobConf sortJob = new JobConf(getConf(), PageRank.class);
		sortJob.setJobName("sortjob");
		sortJob.setMapperClass(SortMap.class);
		sortJob.setOutputKeyClass(DoubleWritable.class);
		sortJob.setOutputValueClass(Text.class);
		sortJob.setInputFormat(SequenceFileAsTextInputFormat.class);
		SequenceFileAsTextInputFormat.setInputPaths(sortJob, tmpDirs.lastElement());
		FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
		JobClient.runJob(sortJob);
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(res);
	}

}
