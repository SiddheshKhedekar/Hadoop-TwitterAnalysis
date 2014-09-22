package kmeanscluster;

import java.io.IOException;

//import kmeanscluster.processor.centroids;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends
		Mapper<Object, Text, IntWritable, IntWritable> {

	float[] centres;
	int cluster;

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		cluster = conf.getInt("cluster", 3);
		centres = new float[cluster];
		for (int i = 0; i < cluster; i++) {
			centres[i] = conf.getFloat("center" + i, 0);
		}
		// System.out.println("centres :: "+ centres[0] + " :: " + centres[1] +
		// " :: " + centres[2] );
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		if (value.toString().equalsIgnoreCase(""))
			return;
		String tokens[] = value.toString().split("\\s+");
		if(tokens.length < 2 || !tokens[1].matches("[-+]?\\d*\\.?\\d+")) 
			return;
//		System.out.println(value.toString() + " :: " + tokens[0]);
//		System.out.println(tokens[1]);
		Integer followerCount = Integer.parseInt(tokens[1]);
		float minimum = Math.abs(centres[0] - followerCount);
		int newCluster = 1;
		for (int i = 1; i < centres.length; i++) {
			if (Math.abs(centres[i] - followerCount) < minimum) {
				newCluster = i + 1;
				minimum = Math.abs(centres[i] - followerCount);
			}
		}
		context.write(new IntWritable(newCluster), new IntWritable(
				followerCount));
	}
}