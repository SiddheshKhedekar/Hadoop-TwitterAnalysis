package shortestpath;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


import shortestpath.processor.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	private Map<Integer, Integer> nodeMap;
	
	public void setup(Context context) {
		nodeMap = new HashMap<Integer, Integer>();
	}

	public void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
	
		String[] tokens;
		int nodeNo = key.get();
		int minDist = 1000;
		int newDist;
		String adjList = null;
		boolean first = true;
		for (Text val : values) {
			tokens = val.toString().split("\\s+");
			newDist = Integer.parseInt(tokens[0]);
			if(tokens.length == 2) {
				adjList = tokens[1];
				nodeMap.put(nodeNo, newDist);
			}
			if(first) {
				minDist = Integer.parseInt(tokens[0]);
				first = false;
			} else {				
				minDist = minDist > newDist ? newDist : minDist;
			}
		}
		
		if (Math.abs(minDist - nodeMap.get(nodeNo)) > 0) {
			context.getCounter(count.nodeNotMatch).increment(1);
		}
		context.write(key,
				new Text(minDist + " " + adjList));
	}

}