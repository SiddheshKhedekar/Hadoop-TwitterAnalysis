package wordcooccurencepairs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.cleanwords;
import util.pair;

public class TokenizerMapper extends Mapper<Object, Text, pair, FloatWritable> {

	private pair wordPair;
	private Map<pair, Float> countHolder;

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// System.out.println("value from documents :: " + value.toString());
		createHashMap(value);
		if (countHolder != null && countHolder.size() > 2000) {
			cleanHolder(context);
		}
	}

	private void createHashMap(Text value) {
		if (countHolder == null) {
			countHolder = new HashMap<pair, Float>();
		}
		StringTokenizer itr = new StringTokenizer(value.toString());
		List<String> pairMap = new ArrayList<String>();
		while (itr.hasMoreTokens()) {
			String wordCleaned = cleanwords.cleaner(itr.nextToken(), "#");
			if (wordCleaned != null && wordCleaned.length() > 0) {
				pairMap.add(wordCleaned);
			}
		}
		storeMap(pairMap);
	}

	private void storeMap(List<String> pairMap) {
		if (pairMap != null && pairMap.size() > 0) {
			for (int i = 0; i < pairMap.size(); i++) {
				String term = pairMap.get(i);
				float count = 0;
				for (int j = 0; j < pairMap.size(); j++) {
					if (i == j) {
						continue;
					} else {
						++count;
						String neighbour = pairMap.get(j);
						wordPair = new pair();
						wordPair.set(term, neighbour);
						if (countHolder.containsKey(wordPair)) {
							countHolder.put(wordPair,
									countHolder.get(wordPair) + 1);
						} else {
							countHolder.put(wordPair, (float)1.0);
						}
					}

				}
//				System.out.println("Mapper Pals Checking :: "+ term + " :: " +count);
				if (count > 0) {
//					System.out.println("Mapper Pals Adding :: "+ term + " :: " +count);
					wordPair = new pair();
					wordPair.set(term, "#*");
					if (countHolder.containsKey(wordPair)) {
						countHolder.put(wordPair, countHolder.get(wordPair)
								+ count);
					} else {
						countHolder.put(wordPair, count);
					}
				}
			}
		}
	}

	private void cleanHolder(Context context) {
		try {
			if (countHolder != null && countHolder.size() > 0) {
				FloatWritable tempCount = new FloatWritable();
				for (Map.Entry<pair, Float> entry : countHolder.entrySet()) {
					wordPair = entry.getKey();
					tempCount.set(entry.getValue());
//					System.out.println("Mapper :: " + wordPair + " :: "
//							+ tempCount);
					context.write(wordPair, tempCount);
				}
				wordPair = new pair();
				countHolder = new HashMap<pair, Float>();
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