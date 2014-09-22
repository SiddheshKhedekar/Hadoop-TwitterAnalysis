package atwordcountfaster;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.cleanwords;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable count = new IntWritable(1);
	private Text word = new Text();
	private Map<String, Integer> countHolder;

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// System.out.println("value from documents :: " + value.toString());
		if(countHolder == null) {
			countHolder = new HashMap<String, Integer>();
		}
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String wordCleaned = cleanwords.cleaner(itr.nextToken(),"@");
			if (wordCleaned != null && wordCleaned.length() > 0) {
				if (countHolder.containsKey(wordCleaned)) {
					countHolder.put(wordCleaned,
							countHolder.get(wordCleaned) + 1);
				} else {
					countHolder.put(wordCleaned, 1);
				}
			}
		}
		if (countHolder.size() > 2000) {
			cleanHolder(context);
		}
	}

	private void cleanHolder(Context context) {
		try {
			if (countHolder != null && countHolder.size() > 0) {
				for (Map.Entry<String, Integer> entry : countHolder.entrySet()) {
					word.set(entry.getKey());
					count.set(entry.getValue());
					context.write(word, count);
				}
				countHolder = new HashMap<String, Integer>();
			}			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void cleanup(Context context) {
		cleanHolder(context);
	}
}