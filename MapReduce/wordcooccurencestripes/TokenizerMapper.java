package wordcooccurencestripes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.cleanwords;
import util.strip;

public class TokenizerMapper extends Mapper<Object, Text, Text, strip> {

	strip stripeHolder;
	Text term;
	Text neighbour;	

	// private final static IntWritable count = new IntWritable(1);
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// System.out.println("value from documents :: " + value.toString());
		createHashMap(value, context);

	}

	private void createHashMap(Text value, Context context) {
		StringTokenizer itr = new StringTokenizer(value.toString());
		List<String> pairMap = new ArrayList<String>();
		while (itr.hasMoreTokens()) {
			String wordCleaned = cleanwords.cleaner(itr.nextToken(), "#");
			if (wordCleaned != null && wordCleaned.length() > 0) {
				pairMap.add(wordCleaned);
			}
		}
		storeMap(pairMap, context);
	}

	private void storeMap(List<String> pairMap, Context context) {
		if (pairMap != null && pairMap.size() > 0) {
			for (int i = 0; i < pairMap.size(); i++) {
				term = new Text(pairMap.get(i));
				stripeHolder = new strip();
				for (int j = 0; j < pairMap.size(); j++) {
					if (i == j) {
						continue;
					} else {
						neighbour = new Text(pairMap.get(j));
						stripeHolder.setCount(neighbour);
					}
				}
				cleanHolder(context);
			}
		}
	}

	private void cleanHolder(Context context) {
		try {
			if (term != null && stripeHolder != null && stripeHolder.size() > 0) {
				context.write(term, stripeHolder);
				term = new Text();
				stripeHolder = new strip();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public void cleanup(Context context) {
		cleanHolder(context);
	}
}