package shortestpath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class processor {

	private static final transient Logger LOG = LoggerFactory
			.getLogger(processor.class);
	static boolean continueIter;
	static int iteration;

	public enum count {
		nodeNotMatch
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		iteration = 0;

		continueIter = true;

		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));

		/* Set the Input/Output Paths on HDFS */
		String inputPath = "/input";
		String outputPath = "/output";

		
		while (continueIter) {

			/*
			 * FileOutputFormat wants to create the output directory itself. If it
			 * exists, delete it:
			 */
			deleteFolder(conf, outputPath);
			Job job = Job.getInstance(conf);
			job.setJarByClass(processor.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.waitForCompletion(true);
			continueIter = job.getCounters().findCounter(count.nodeNotMatch)
					.getValue() == 0 ? false : true;
			System.out.println("Counter :: "
					+ job.getCounters().findCounter(count.nodeNotMatch)
							.getValue());
			iteration++;
			System.out.println("Iteration :: " + iteration);
			deleteFiles(conf, inputPath);
			copyFolder(conf, outputPath, inputPath);
		}
	}

	private static void deleteFiles(Configuration conf, String inputPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path inpath = new Path(inputPath);
		if (fs.exists(inpath)) {
			FileStatus[] status = fs.listStatus(inpath);
			if (status == null || status.length < 1) {
				System.out.println("No input files to delete");
				System.exit(1);
			} else {
				for (int i = 0; i < status.length; i++) {
					fs.delete(status[i].getPath(), true);
				}
			}
		}
	}

	private static void copyFolder(Configuration conf, String outputPath,
			String inputPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		List<Path> outpaths = new ArrayList<Path>();
		Path outpath = new Path(outputPath);
		Path inpath = new Path(inputPath);
		if (fs.exists(outpath)) {
			FileStatus[] status = fs.listStatus(outpath);
			if (status == null || status.length < 1) {
				System.out.println("No output files to process copy");
				System.exit(1);
			} else {
				for (int i = 0; i < status.length; i++) {
					if (status[i].getPath().getName().contains("SUCCESS")
							|| status[i].getPath().getName()
									.contains("FAILURE")) {
						continue;
					}
					outpaths.add(status[i].getPath());
				}
			}
			Path[] outPathArr = new Path[outpaths.size()]; 
			outPathArr = outpaths.toArray(outPathArr);
			FileUtil.copy(fs, outPathArr, fs, inpath, false,
					true, conf);

		}
	}

	/**
	 * Delete a folder on the HDFS. This is an example of how to interact with
	 * the HDFS using the Java API. You can also interact with it on the command
	 * line, using: hdfs dfs -rm -r /path/to/delete
	 * 
	 * @param conf
	 *            a Hadoop Configuration object
	 * @param folderPath
	 *            folder to delete
	 * @throws IOException
	 */
	private static void deleteFolder(Configuration conf, String folderPath)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}
}