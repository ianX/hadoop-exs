package org.rs.servernode.protocol;

public class Properties {

	public final static String[] NODE_IDS = { Messages.getString("Properties.id0"), Messages.getString("Properties.id1"), Messages.getString("Properties.id2"), Messages.getString("Properties.id3") }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

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

	public final static String MOVIE_FILE_PREFIX = Messages
			.getString("Properties.movie_prefix"); //$NON-NLS-1$
	public final static String USER_FILE_PREFIX = Messages
			.getString("Properties.user_prefix"); //$NON-NLS-1$
	public final static String MOVIE_TITLES = Messages
			.getString("Properties.movie_titles"); //$NON-NLS-1$

	// io params
	static public int NUM_BLOCK = 4;
	static public int num_iter = 10;
	static public double step_len = 2;
	static public double reg_ = 2;
	static public double shrink = 0.95;
	static public double INFINITY = 1e9;
	static public int K = 10;
	static public int MAX_NUM_LIST = 20;
	static public int MAX_NUM_REC = 10;
	static public int RANDOM_MAX = 100;
	static public int ROW_SIZE = 18000;
	static public int COL_SIZE = 10000000;

	static public String BLOCK_PATH = "block"; //$NON-NLS-1$
	static public String INITIAL_V = "0"; //$NON-NLS-1$
	static public String INITIAL_U = "1"; //$NON-NLS-1$
	static public String SEP_RATE = "]"; //$NON-NLS-1$
	static public String REG_SEP_RATE = "\\]"; //$NON-NLS-1$
	static public String SEP_ID_RATE = ","; //$NON-NLS-1$
	static public String REG_SEP_ID_RATE = ","; //$NON-NLS-1$
	static public String SEP_LATENT_VALUE = ","; //$NON-NLS-1$
	static public String REG_SEP_LATENT_VALUE = ","; //$NON-NLS-1$
	static public String SEP_ID_VECTOR = "\t"; //$NON-NLS-1$
	static public String REG_SEP_ID_VECTOR = "\t"; //$NON-NLS-1$

}
