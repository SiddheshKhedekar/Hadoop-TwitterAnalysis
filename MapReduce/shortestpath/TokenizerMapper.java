package shortestpath;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		if (value.toString().equalsIgnoreCase(""))
			return;
		String tokens[] = value.toString().split("\\s+");
		int distance = Integer.parseInt(tokens[1]);
		String nodes[] = tokens[2].split(":");
		for (String node : nodes) {
			context.write(new IntWritable(Integer.parseInt(node)), new Text(
					Integer.toString(distance + 1)));
		}
		context.write(new IntWritable(Integer.parseInt(tokens[0])), new Text(
				tokens[1] + " " + tokens[2]));
	}
}