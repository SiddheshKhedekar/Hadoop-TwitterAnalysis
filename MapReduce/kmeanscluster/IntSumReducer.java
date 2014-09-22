package kmeanscluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//import kmeanscluster.processor.centroids;


import kmeanscluster.processor.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends
		Reducer<IntWritable, IntWritable, Text, Text> {

	private Map<Integer, Float> centroidMap;
	float[] oldcentres;
	int cluster;
	
	public void setup(Context context) {
		centroidMap = new HashMap<Integer, Float>();
		Configuration conf = context.getConfiguration();
		cluster = conf.getInt("cluster", 3);
		oldcentres = new float[cluster];
		for (int i = 0; i < cluster; i++) {
			oldcentres[i] = conf.getFloat("center" + i, 0);
		}
		// System.out.println("oldcentres :: "+ oldcentres[0] + " :: " +
		// oldcentres[1] + " :: " + oldcentres[2] );
	}

	public void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		int ctr = 0;
		int ptr;
		StringBuilder ptrList = new StringBuilder();

		for (IntWritable val : values) {
			ptr = val.get();
			sum += ptr;
			ptrList.append(ptr + " , ");
			ctr++;
		}
		float centre = (float) (sum / ctr);
		// update counters with centroids
		int centreNum = key.get();
		centroidMap.put(centreNum, centre);
		context.write(new Text(centreNum + " " + centre),
				new Text(ptrList.toString()));
	}

	public void cleanup(Context context) {
		for (int i = 0; i < oldcentres.length; i++) {
			if (Math.abs(oldcentres[i] - centroidMap.get(i + 1)) > 1) {
				context.getCounter(count.centreNotMatch).increment(1);
			}
		}
	}
}