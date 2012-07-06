package org.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.pagerank.io.LinkWritable;
import org.pagerank.io.PRArrayWritable;

public class GraphGenerator {

	public static class Map extends
			Mapper<LongWritable, Text, Text, LinkWritable> {

		private static final Pattern ptitle = Pattern
				.compile("&lttitle&gt(.*)&lt/title&gt");
		private static final Pattern plink = Pattern
				.compile("\\[\\[([\\p{Print}&&[^\\]]]*)\\]\\]");

		private Text outKey = new Text();
		private LinkWritable outVal = new LinkWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String val = value.toString();
			Matcher mtitle = ptitle.matcher(val);
			if (mtitle.find()) {
				String title = mtitle.group(1);
				Matcher mlink = plink.matcher(val.subSequence(
						val.indexOf("&lttext[\\p{Print}&&[^&]]*&gt")+1,
						val.indexOf("&lt/text&gt")));
				String[] links;
				while (mlink.find()) {
					links = mlink.group(1).split("\\|");
					for (String link : links) {
						outKey.set(title);
						outVal.set(true, link);
						context.write(outKey, outVal);
						outKey.set(link);
						outVal.set(false, title);
						context.write(outKey, outVal);
					}
				}
			}
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
			int outdegree = 0;
			vals.clear();
			for (LinkWritable val : value) {
				if (val.getBoolean())
					outdegree++;
				vals.add(val);
			}
			outVal.set(1, outdegree,
					vals.toArray(new LinkWritable[vals.size()]));
			context.write(key, outVal);
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
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(PRArrayWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			TextInputFormat.addInputPath(job, new Path(args[0]));
			SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
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
