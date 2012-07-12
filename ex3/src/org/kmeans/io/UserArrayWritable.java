package org.kmeans.io;

import org.apache.hadoop.io.ArrayWritable;

public class UserArrayWritable extends ArrayWritable {
	public UserArrayWritable() {
		// TODO Auto-generated constructor stub
		super(UserWritable.class);
	}
}
