package org.rs.servernode.protocol;

public class Properties {

	// sm protocols
	public final static int REC_NUM = 10;
	public final static long NODE_DEAD = 5000;
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

	public final static int IM_DEAD = HEART_BEAT + 1;

	public final static String MOVIE_FILE_PREFIX = "movie*";
	public final static String USER_FILE_PREFIX = "user*";

	// io params
	static public int NUM_BLOCK = 4;
	static public int num_iter = 10;
	static public double step_len = 2;
	static public double reg_ = 2;
	static public double shrink = 0.95;
	static public double INFINITY = 1e9;
	static public int K = 8;
	static public int MAX_NUM_LIST = 10;
	static public int MAX_NUM_REC = 1;
	static public int RANDOM_MAX = 1;
	static public int ROW_SIZE = 18000;
	static public int COL_SIZE = 10000000;

	static public String BLOCK_PATH = "block";
	static public String INITIAL_V = "0";
	static public String INITIAL_U = "1";
	static public String SEP_RATE = "]";
	static public String REG_SEP_RATE = "\\]";
	static public String SEP_ID_RATE = ",";
	static public String REG_SEP_ID_RATE = ",";
	static public String SEP_LATENT_VALUE = ",";
	static public String REG_SEP_LATENT_VALUE = ",";
	static public String SEP_ID_VECTOR = "\t";
	static public String REG_SEP_ID_VECTOR = "\t";

}
