package org.ianX.util;

public class StringSpliter {

	private char[] string = null;
	private char spliter = ' ';
	private int offset = 0;
	private int length = 0;

	public StringSpliter() {
		// TODO Auto-generated constructor stub
		string = new char[0];
	}

	public StringSpliter(String string, char spliter) {
		this.string = string.toCharArray();
		this.length = string.length();
		this.spliter = spliter;
	}

	public void set(String string, char spliter) {
		this.string = string.toCharArray();
		this.length = string.length();
		this.spliter = spliter;
		offset = 0;
	}

	public void changeSpliter(char spliter) {
		this.spliter = spliter;
	}

	public String next() {

		if (offset >= length)
			return null;

		int count = offset;
		while (++count < length && string[count] != spliter)
			;

		String ret = String.copyValueOf(string, offset, count - offset);

		offset = count;
		while (++offset < length && string[offset] == spliter)
			;

		return ret;
	}

	public String left() {
		if (offset >= length)
			return null;
		return String.copyValueOf(string, offset, length - offset);
	}

	public void reset() {
		offset = 0;
	}

	public int getOffset() {
		if (offset > length)
			return length;
		return offset;
	}

	public int genLength() {
		return length;
	}
}
