package org.rs.servernode.protocol;

public class Properties {
	public final static int REC_NUM = 10;
	public final static long SLEEP_TIME = 1000;
	public final static long WAITE_TIME = 1000;
	public final static int MASTER_PORT = 40030;
	public final static int SLAVE_PORT = 40035;
	public final static int ADD_FILES = 0;
	public final static int FILES_TO_REOMVE = ADD_FILES + 1;
	public final static int REMOVE_MARKED_FILES = FILES_TO_REOMVE + 1;
	public final static int MOVIE_LIST = REMOVE_MARKED_FILES + 1;
	public final static int REC_MOVIE = MOVIE_LIST + 1;
	public final static int REC_USER = REC_MOVIE + 1;
	public final static int MOVIE_VECTOR = REC_USER + 1;
	public final static int USER_VECTOR = MOVIE_VECTOR + 1;
	public final static int HEART_BEAT = USER_VECTOR + 1;
}
