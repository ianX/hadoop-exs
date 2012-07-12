package org.pagerank.io;
import org.apache.hadoop.io.ArrayWritable;

public class LinkArrayWritable extends ArrayWritable {

	public LinkArrayWritable() {
		super(LinkWritable.class);
	}
}
