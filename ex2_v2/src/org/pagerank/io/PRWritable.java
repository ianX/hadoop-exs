package org.pagerank.io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class PRWritable implements Writable {

	private DoubleWritable pr;
	private LinkWritable links;

	public PRWritable() {
		// TODO Auto-generated constructor stub
		set(new DoubleWritable(), new LinkWritable());
	}

	public PRWritable(double d, LinkWritable links) {
		set(new DoubleWritable(d), links);
	}

	public PRWritable(DoubleWritable d, LinkWritable links) {
		set(d, links);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.pr.readFields(in);
		this.links.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.pr.write(out);
		this.links.write(out);
	}

	public void set(double pr, LinkWritable lns) {
		this.pr.set(pr);
		this.links = lns;
	}

	public void set(DoubleWritable pr, LinkWritable lns) {
		this.pr = pr;
		this.links = lns;
	}

	public double getDouble() {
		return this.pr.get();
	}

	public LinkWritable getLink() {
		return this.links;
	}

}
