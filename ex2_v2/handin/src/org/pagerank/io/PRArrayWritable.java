package org.pagerank.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class PRArrayWritable implements Writable {

	private DoubleWritable pr;
	private IntWritable outdegree;
	private LinkArrayWritable links;

	public PRArrayWritable() {
		// TODO Auto-generated constructor stub
		set(new DoubleWritable(), new IntWritable(), new LinkArrayWritable());
	}

	public PRArrayWritable(double d, int i, LinkWritable[] links) {
		LinkArrayWritable ln = new LinkArrayWritable();
		ln.set(links);
		set(new DoubleWritable(d), new IntWritable(i), ln);
	}

	public PRArrayWritable(DoubleWritable d, IntWritable i,
			LinkArrayWritable links) {
		set(d, i, links);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.pr.readFields(in);
		this.outdegree.readFields(in);
		this.links.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.pr.write(out);
		this.outdegree.write(out);
		this.links.write(out);
	}

	public void set(double pr, int i, LinkWritable[] links) {
		this.pr.set(pr);
		this.outdegree.set(i);
		this.links.set(links);
	}

	public void set(DoubleWritable pr, IntWritable i, LinkArrayWritable lns) {
		this.pr = pr;
		this.outdegree = i;
		this.links = lns;
	}

	public double getDouble() {
		return this.pr.get();
	}

	public DoubleWritable getDoubleWritable() {
		return this.pr;
	}

	public int getInt() {
		return this.outdegree.get();
	}

	public LinkArrayWritable getLinkArray() {
		return this.links;
	}

}
