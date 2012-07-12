package org.pagerank.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LinkWritable implements Writable {

	private BooleanWritable positive;
	private Text link;

	public LinkWritable() {
		set(new BooleanWritable(), new Text());
	}

	public LinkWritable(boolean b, String s) {
		set(new BooleanWritable(b), new Text(s));
	}

	public LinkWritable(BooleanWritable b, Text s) {
		set(b, s);
	}

	public LinkWritable(LinkWritable l) {
		set(new BooleanWritable(l.getBoolean()), new Text(l.getText()));
		// set(l.getBooleanWritable(), l.getText());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.positive.readFields(in);
		this.link.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.positive.write(out);
		this.link.write(out);
	}

	public boolean getBoolean() {
		return this.positive.get();
	}

	public BooleanWritable getBooleanWritable() {
		return this.positive;
	}

	public Text getText() {
		return this.link;
	}

	public void set(boolean b, String s) {
		this.positive.set(b);
		this.link.set(s);
	}

	public void set(BooleanWritable b, Text s) {
		this.positive = b;
		this.link = s;
	}
}
