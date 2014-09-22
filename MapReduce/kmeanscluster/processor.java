package kmeanscluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
	static int cluster;
	static float[] currentCentres;
	static int iteration;

	public enum count {
		centreNotMatch
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		// Code currently works for three clusters
		cluster = 3;
		iteration = 25;
		currentCentres = new float[cluster];

		continueIter = true;

		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));

		/* Set the Input/Output Paths on HDFS */
		String inputPath = "/input";
		String outputPath = "/output";
		/*
		 * Read the first k entries from file as centres.
		 */
		currentCentres = configureCentroids(conf, inputPath);

		while (continueIter && iteration > 0) {

			/*
			 * FileOutputFormat wants to create the output directory itself. If
			 * it exists, delete it:
			 */
			deleteFolder(conf, outputPath);
			conf = setConf(conf);
			Job job = Job.getInstance(conf);
			job.setJarByClass(processor.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.waitForCompletion(true);
			continueIter = job.getCounters().findCounter(count.centreNotMatch)
					.getValue() == 0 ? false : true;
			System.out.println("Counter :: "
					+ job.getCounters().findCounter(count.centreNotMatch)
							.getValue());
			System.out.println("Iteration :: "+ (26-iteration));
			currentCentres = configureNewCentroids(conf, outputPath);
			System.out.print("Current Centers :: ");
			for (int i = 0; i < cluster; i++) {
				System.out.print(currentCentres[i]+ " , ");
			}
			System.out.println();
			iteration--;
		}
	}

	private static Configuration setConf(Configuration conf) {
		for (int i = 0; i < cluster; i++) {
			conf.setFloat("center" + i, currentCentres[i]);
		}
		conf.setInt("cluster", cluster);
		return conf;
	}

	private static float[] configureCentroids(Configuration conf,
			String inputPath) throws IOException {
		float[] centres = new float[cluster];
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(inputPath);
		if (fs.exists(path)) {
			FileStatus[] status = fs.listStatus(path);
			if (status == null || status.length < 1) {
				System.out.println("No input files to process");
				System.exit(1);
			} else {
				FileStatus firstStatus = status[0];
				FSDataInputStream fsdis = fs.open(firstStatus.getPath());
				InputStreamReader isr = new InputStreamReader(fsdis);
				BufferedReader br = new BufferedReader(isr);
				int i = 0;
				String line = br.readLine();
				while (line != null && i < cluster) {
					String tokens[] = line.toString().split("\\s+");
					centres[i] = Float.parseFloat(tokens[1]);
					i++;
					line = br.readLine();
				}
				br.close();
			}
		}
		return centres;
	}

	private static float[] configureNewCentroids(Configuration conf,
			String outputPath) throws IOException {
		float[] centres = new float[cluster];
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path(outputPath);
		if (fs.exists(path)) {
			// System.out.println("Output path do exist");
			FileStatus[] status = fs.listStatus(path);
			if (status == null || status.length < 1) {
				System.out.println("No output files to process");
				System.exit(1);
			} else {
				FileStatus firstStatus = status[0];
				if (firstStatus.getPath().getName().contains("SUCCESS")
						|| firstStatus.getPath().getName().contains("FAILURE")) {
					firstStatus = status[1];
				}
				// System.out.println(firstStatus.getPath().getName());
				FSDataInputStream fsdis = fs.open(firstStatus.getPath());
				InputStreamReader isr = new InputStreamReader(fsdis);
				BufferedReader br = new BufferedReader(isr);
				int i = 0;
				String line = br.readLine();
				String[] tokens;
				while (line != null && i < cluster) {
//					System.out.println(line);
					tokens = line.split("\\s+");
					// System.out.println(tokens[1]);
					if(tokens.length < 2 || !tokens[1].matches("[-+]?\\d*\\.?\\d+")) 
						continue;
					centres[i] = Float.parseFloat(tokens[1]);
					i++;
					line = br.readLine();
				}
				br.close();
			}
		}
		return centres;
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