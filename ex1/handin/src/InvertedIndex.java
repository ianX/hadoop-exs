import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
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
		private int docId = -1;

		public void configure(JobConf job) {
			try {

				fname = job.get("map.input.file");
				fname = fname.substring(fname.lastIndexOf('/') + 1);

				String files = job.get("invertedindex.filename.map");

				FSDataInputStream is = FileSystem.get(job)
						.open(new Path(files));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(is));
				String pair;
				while ((pair = br.readLine()) != null) {
					String[] token = pair.split(":");
					if (fname.equals(token[1])) {
						docId = Integer.parseInt(token[0]);
						break;
					}
				}

				br.close();

				if (useStopword = job.getBoolean("invertedindex.use.stopword",
						false)) {
					Path[] stopwordFiles = DistributedCache
							.getLocalCacheFiles(job);
					for (Path stopwordFile : stopwordFiles) {
						parseStopwordFile(stopwordFile);
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private void parseStopwordFile(Path path) throws IOException {
			BufferedReader fis = new BufferedReader(new FileReader(
					path.toString()));
			String word = null;
			while ((word = fis.readLine()) != null) {
				stopword.add(word);
			}
			fis.close();
		}

		private String pattern = "\\p{Punct}";
		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String valTmp = value.toString().toLowerCase()
					.replaceAll("\'s ", " ");
			StringTokenizer tokens = new StringTokenizer(valTmp.replaceAll(
					pattern, " "));
			int pos = 0;
			while (tokens.hasMoreTokens()) {
				String token = tokens.nextToken();
				if (useStopword
						&& (stopword.contains(token) || token.matches("[0-9]*"))) {
					pos++;
					continue;
				}
				if (docId != -1)
					outKey.set(token + ":" + docId);
				else
					outKey.set(token + ":" + fname);
				outVal.set("1" + ":" + (pos++));
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
			outVal.set("<" + keys[1] + ">" + "[" + sum + "]" + pos);
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

	public static class SearchMap extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		private HashMap<String, String> filemap = new HashMap<String, String>();
		private String[] words;

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			words = job.get("search.word").split("[ \t]+");
			try {
				Path[] filemapcache = DistributedCache.getLocalCacheFiles(job);
				for (Path file : filemapcache) {
					BufferedReader fis = new BufferedReader(new FileReader(
							file.toString()));
					parseFileMap(fis, filemap);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			for (String word : words) {
				if (key.toString().equals(word)) {
					String[] outVals = parseOutValue(value.toString(), filemap)
							.split("\n");
					for (String val : outVals) {
						String outkey = val.substring(0, val.indexOf('['));
						String outval = word + "::"
								+ val.substring(val.indexOf(':') + 1);
						output.collect(new Text(outkey), new Text(outval));
					}
					break;
				}
			}
		}

	}

	public static class SearchReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private String[] words;

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			words = job.get("search.word").split("[ \t]+");
		}

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			int i = 0;
			StringBuffer outval = new StringBuffer();
			while (value.hasNext()) {
				outval.append("\n\t" + value.next().toString());
				i++;
			}
			if (i == words.length) {
				output.collect(key, new Text(outval.toString()));
			}
		}

	}

	public static void parseFileMap(BufferedReader fis,
			HashMap<String, String> filemap) throws IOException {
		String line = null;
		while ((line = fis.readLine()) != null) {
			String[] pair = line.split(":");
			filemap.put(pair[0], pair[1]);
		}
		fis.close();
	}

	public static String parseOutValue(String value,
			HashMap<String, String> filemap) {
		String val = value.replace("->", "");
		Pattern p = Pattern.compile("<([0-9]*)>");
		Matcher m = p.matcher(val);
		String doc;
		while (m.find()) {
			doc = m.group(1);
			if (filemap.containsKey(doc)) {
				doc = filemap.get(doc);
			}
			val = m.replaceFirst(doc);
			m = p.matcher(val);
		}
		String outVal = val.trim().replaceAll(" *, *", "\n");
		return outVal;
	}

	public static void parseInvertedIndex(BufferedReader fis,
			HashMap<String, String> invertedindex) throws IOException {
		String line = null;
		while ((line = fis.readLine()) != null) {
			String[] pair = line.split("\t");
			invertedindex.put(pair[0], pair[1]);
		}
		fis.close();
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Path inpath = null;
		Path outpath = null;
		Path filemap = null;
		boolean isSearch = false;
		boolean useMR = false;
		boolean useFilemap = false;
		boolean indexonly = false;

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-search")) {
				isSearch = true;
			} else if (args[i].toLowerCase().equals("-usemr")) {
				useMR = true;
			} else if (args[i].toLowerCase().equals("-filemap")) {
				filemap = new Path(args[++i]);
				useFilemap = true;
			} else if (args[i].toLowerCase().equals("-indexonly")) {
				indexonly = true;
			} else if (args[i].contains("-")) {
				continue;
			}
			other_args.add(args[i]);
		}

		inpath = new Path(other_args.get(0));
		outpath = new Path(other_args.get(1));
		if (!useFilemap)
			filemap = new Path(outpath.getParent().toString() + "/filemap.txt");

		if (!isSearch) {
			JobConf conf = new JobConf(getConf(), InvertedIndex.class);
			if (args.length == 3) {
				Path stop_word_path = new Path(other_args.get(2));
				if (FileSystem.get(conf).isFile(stop_word_path)) {
					DistributedCache.addCacheFile(stop_word_path.toUri(), conf);
				} else {
					FileStatus[] filestatus = FileSystem.get(conf).globStatus(
							new Path(other_args.get(2) + "/p*"));
					for (FileStatus fs : filestatus) {
						DistributedCache.addCacheFile(fs.getPath().toUri(),
								conf);
					}
				}
				conf.setBoolean("invertedindex.use.stopword", true);
			}
			conf.setJobName("inverted index");
			conf.setMapperClass(Map.class);
			conf.setCombinerClass(Combine.class);
			conf.setReducerClass(Reduce.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setInputFormat(MyInputFormat.class);

			MyInputFormat.setInputPaths(conf, inpath);
			FileOutputFormat.setOutputPath(conf, outpath);

			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream os = fs.create(filemap);

			FileStatus[] inputnames = fs.globStatus(new Path(other_args.get(0)
					+ "/*"));
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os));
			for (int i = 0; i < inputnames.length; i++) {
				br.append(i + ":" + inputnames[i].getPath().getName());
				br.newLine();
			}
			br.close();
			conf.set("invertedindex.filename.map", filemap.toString());
			JobClient.runJob(conf);
		}

		if (indexonly)
			return 0;

		String word;
		BufferedReader ibr = new BufferedReader(
				new InputStreamReader(System.in));
		if (useMR) {
			System.out.print("input some words(or 'q' to exit): ");
			Path searchOutput = new Path(outpath.getParent().toString()
					+ "/search");
			while (!(word = ibr.readLine()).equals("q")) {
				JobConf searchJob = new JobConf(getConf(), InvertedIndex.class);
				DistributedCache.addCacheFile(filemap.toUri(), searchJob);
				searchJob.setJobName("Search");
				searchJob.set("search.word", word.toLowerCase());
				searchJob.setMapperClass(SearchMap.class);
				searchJob.setReducerClass(SearchReduce.class);
				searchJob.setInputFormat(KeyValueTextInputFormat.class);
				searchJob.setOutputKeyClass(Text.class);
				searchJob.setOutputValueClass(Text.class);
				KeyValueTextInputFormat.setInputPaths(searchJob, outpath);
				FileOutputFormat.setOutputPath(searchJob, searchOutput);
				JobClient.runJob(searchJob);
				FileSystem sfs = FileSystem.get(searchJob);
				FileStatus[] sout = sfs.globStatus(new Path(searchOutput
						.toString() + "/p*"));
				for (FileStatus sfile : sout) {
					FSDataInputStream sin = FileSystem.get(searchJob).open(
							sfile.getPath());
					BufferedReader sbr = new BufferedReader(
							new InputStreamReader(sin));
					String pr;
					while ((pr = sbr.readLine()) != null)
						System.out.println(pr);
					sbr.close();
				}
				sfs.delete(searchOutput, true);
				System.out.print("input some words(or 'q' to exit): ");
			}
		} else {
			FileSystem fs = FileSystem.get(new Configuration());
			HashMap<String, String> fidpair = new HashMap<String, String>();
			HashMap<String, String> invertedindex = new HashMap<String, String>();
			FSDataInputStream is = fs.open(filemap);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			parseFileMap(br, fidpair);
			br.close();
			FileStatus[] iis = fs.globStatus(new Path(outpath.toString()
					+ "/p*"));
			for (FileStatus ii : iis) {
				is = fs.open(ii.getPath());
				br = new BufferedReader(new InputStreamReader(is));
				parseInvertedIndex(br, invertedindex);
				br.close();
			}

			System.out.print("input a word(or 'q' to exit): ");
			String line;
			while (!(line = ibr.readLine()).equals("q")) {
				if (invertedindex.containsKey(line)) {
					String val = invertedindex.get(line);
					String outVal = parseOutValue(val, fidpair);
					System.out.println(outVal);
				}
				System.out.print("input a word(or 'q' to exit): ");
			}
		}
		ibr.close();
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
