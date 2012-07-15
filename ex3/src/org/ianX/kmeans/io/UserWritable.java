package org.ianX.kmeans.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class UserWritable implements Writable {

	Text name;
	IntWritable rating;

	public UserWritable() {
		// TODO Auto-generated constructor stub
		set(new Text(), new IntWritable());
	}

	public UserWritable(String name, int rating) {
		set(new Text(name), new IntWritable(rating));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.name.readFields(in);
		this.rating.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.name.write(out);
		this.rating.write(out);
	}

	public void set(Text name, IntWritable rating) {
		this.name = name;
		this.rating = rating;
	}

	public void set(String name, int rating) {
		this.name.set(name);
		this.rating.set(rating);
	}

}
