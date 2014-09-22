package wordcooccurencepairs;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import util.pair;

public class IntSumReducer extends
		Reducer<pair, FloatWritable, pair, FloatWritable> {

	private FloatWritable result;
	float termCount;

	public void reduce(pair key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
//		System.out.println("Reducer :: " + key);
		float sum = 0;
		float relFreq = 0;
		String neg = key.getNeighbour().toString();
		if (neg.equalsIgnoreCase("#*")) {
			termCount = 0;
			for (FloatWritable val : values) {
				termCount += val.get();
			}
		} else {
			for (FloatWritable val : values) {
				sum += val.get();
			}
			relFreq = sum / termCount;
			result = new FloatWritable();
			result.set((Math.round(relFreq*1000f))/1000f);
			// System.out.println("reducer :: "+ key + " :: " + result);
			context.write(key, result);
		}
//		System.out.println("Reducer :: " + key + " :: " + sum + " :: "
//				+ termCount + " :: "+relFreq);
	}
}
