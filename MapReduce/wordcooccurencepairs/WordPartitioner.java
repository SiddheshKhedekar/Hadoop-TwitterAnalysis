package wordcooccurencepairs;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import util.pair;

public class WordPartitioner extends Partitioner<pair, FloatWritable> {

	@Override
	public int getPartition(pair key, FloatWritable value, int numReduceTasks) {
		
		Text term = key.getTerm();
        
		int hashTerm;
		int partNo = 0;
		
		if(numReduceTasks == 0) {
			return partNo;
		}
		
		if(term != null) {
			hashTerm = Math.abs(term.hashCode());
			partNo = hashTerm % numReduceTasks;
		}
//		System.out.println("Partitioner :: "+term + " :: "+partNo); 
		return partNo;
	}

}
