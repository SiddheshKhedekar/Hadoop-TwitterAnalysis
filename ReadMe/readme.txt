1. Install hadoop 
2. start the hadoop nodes : start-dfs.sh
3. start the yarn : start-yarn.sh
4. Deploy the jar assignment2.jar present in code/jar into a local directory
5. Create the input /input in hdfs cluster
	hdfs dfs -mkdir /input
6. Create the output /output in hdfs cluster
	hdfs dfs -mkdir /output
7. To test the word count program 1a
	a. Move the input files from data/1.txt to run the following modules
	b. hadoop jar assignment2.jar simplewordcountfaster.processor
	c. Output will be generated in the /output directory : hdfs dfs -cat /output/*
8. To test the # word count program 1b
	a. Move the input files from data/1.txt to run the following modules
	b. hadoop jar assignment2.jar hashwordcountfaster.processor
	c. Output will be generated in the /output directory : hdfs dfs -cat /output/*
9. To test the @ word count program 1c
	a. Move the input files from data/1.txt to run the following modules
	b. hadoop jar assignment2.jar atwordcountfaster.processor
	c. Output will be generated in the /output directory : hdfs dfs -cat /output/*
10. To test the pairs program 2a
	a. Move the input files from data/1.txt to run the following modules
	b. hadoop jar assignment2.jar wordcooccurencepairs.processor
	c. Output will be generated in the /output directory : hdfs dfs -cat /output/*
11. To test the stripes program 2b
	a. Move the input files from data/1.txt to run the following modules
	b. hadoop jar assignment2.jar wordcooccurencestripes.processor
	c. Output will be generated in the /output directory : hdfs dfs -cat /output/*
12. To test the k means cluster 3
	a. Move the input files from data/3.txt to run the following modules
	b. hadoop jar assignment2.jar kmeanscluster.processor
	c. Output will be generated in the /output directory : hdfs dfs -cat /output/*
13. To test the shortest path 4
	a. Move the input files from data/4.txt to run the following modules
	b. hadoop jar assignment2.jar shortestpath.processor
	c. Output will be generated in the /output directory : hdfs dfs -cat /output/*

