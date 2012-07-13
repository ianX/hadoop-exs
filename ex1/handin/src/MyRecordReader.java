import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

public class MyRecordReader implements RecordReader<LongWritable, Text> {
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	int maxLineLength;
	private boolean hasNext = true;

	public MyRecordReader(Configuration job, FileSplit split)
			throws IOException {
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
			in = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			in = new LineReader(fileIn, job);
		}
		if (skipFirstLine) { // skip first line and re-establish "start".
			start += in.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}

	public MyRecordReader(InputStream in, long offset, long endOffset,
			int maxLineLength) {
		this.maxLineLength = maxLineLength;
		this.in = new LineReader(in);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;
	}

	public MyRecordReader(InputStream in, long offset, long endOffset,
			Configuration job) throws IOException {
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		this.in = new LineReader(in, job);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;
	}

	public LongWritable createKey() {
		return new LongWritable();
	}

	public Text createValue() {
		return new Text();
	}

	/** Read all lines. */
	public synchronized boolean next(LongWritable key, Text value)
			throws IOException {

		if (!hasNext) {
			key = null;
			value = null;
			return false;
		}

		Text val = new Text();
		StringBuffer sb = new StringBuffer();
		key.set(pos);
		while (pos < end) {

			int newSize = in.readLine(val, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			pos += newSize;

			sb.append(val.toString());
		}

		value.set(sb.toString());
		hasNext = false;
		return true;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public synchronized long getPos() throws IOException {
		return pos;
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
}
