用法: 
1.hadoop jar invertedindex.jar <in> <out> -search [<other commands>]
	直接进入query program
	<other commands>包括：
		-usemr	使用MapReduce版的query program(默认为单机版)
		-filemap <filemap path> 指定filemap的(HDFS)路径，默认为<out>所在目录下的filemap.txt
2.hadoop jar invertedindex.jar <in> <out> [<stopword>] [<commands>]
	生成InvertedIndex到<out>目录
	<stopword>用于指定stopword目录，若不存在，则成生stopword放在该目录，若已存在则直接使用
	<commands>包括：
		-usermr 	同上
		-filemap	同上
		-indexonly	只生成InvertedIndex，不进入query program(默认进入单机版)
		
注：-search 和 -indexonly同时使用则什么都不做直接退出。
