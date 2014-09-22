package wordcooccurencestripes;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.strip;



public class IntSumReducer 
extends Reducer<Text, strip,Text,strip> {
	private strip result,count;

	public void reduce(Text key, Iterable<strip> values, Context context) throws IOException, InterruptedException {
		count = new strip();
		for (strip val : values) {
			count.addCount(val);
		}
		result = count.relativecount();
		context.write(key, result);
//		System.out.println("Reducer :: "+ key + " :: " + result);
	}
}
