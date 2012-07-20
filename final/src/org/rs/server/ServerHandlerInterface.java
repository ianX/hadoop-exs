package org.rs.server;

public interface ServerHandlerInterface extends Runnable {

	public static enum RetCode {
		good, close, createDataSocketFailed, closeFailed, sendMovieListFailed, sendRecMovieFailed, sendRecUserFailed, receiveRatingFailed, cmdError
	}

	public RetCode createDataSocket();

	public RetCode close();

	public RetCode cmdError(RetCode retCode);

	public RetCode sendMovieList();

	public RetCode sendRecMovie();

	public RetCode sendRecUser();

	public RetCode receiveRating();

	public RetCode cmdHandler(String cmd);
}
