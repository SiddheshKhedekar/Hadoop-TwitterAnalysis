package util;

import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class strip extends MapWritable {

	public strip() {
		super();
	}

	public void setCount(Text neighbour) {
		FloatWritable count;
		if (this.containsKey(neighbour)) {
			count = (FloatWritable) this.get(neighbour);
			count = new FloatWritable(count.get() + 1);
			this.put(neighbour, count);
		} else {
			count = new FloatWritable(1);
			this.put(neighbour, count);
		}
	}

	public void addCount(strip val) {
		for (Map.Entry entry : val.entrySet()) {
			Text neighbour = (Text) entry.getKey();
			FloatWritable newCount = (FloatWritable) entry.getValue();
			if (this.containsKey(neighbour)) {
				FloatWritable oldCount = (FloatWritable) this.get(neighbour);
				FloatWritable count = new FloatWritable(oldCount.get()
						+ newCount.get());
				this.put(neighbour, count);
			} else {
				this.put(neighbour, newCount);
			}
		}
	}

	public strip relativecount() {
		float count = totalcount();
		strip newStrip = new strip();
		for (Map.Entry entry : this.entrySet()) {
			Text neighbour = (Text) entry.getKey();
			FloatWritable newCount = (FloatWritable) entry.getValue();
			float countVal = newCount.get();
			if (count == 0) {
				newStrip.put(neighbour, new FloatWritable(0));
			} else {
				newStrip.put(neighbour, new FloatWritable(countVal / count));
			}
//			System.out.println(neighbour + " :: "+countVal + " :: "+ count+" :: "+countVal/count);
			
		}
		return newStrip;
	}

	private float totalcount() {
		float count = 0;
		for (Map.Entry entry : this.entrySet()) {
			FloatWritable newCount = (FloatWritable) entry.getValue();
			count += newCount.get();
		}
		return count;
	}

	public String toString() {
		StringBuilder result = new StringBuilder();
		for (Map.Entry entry : this.entrySet()) {
			Text neighbour = (Text) entry.getKey();
			FloatWritable count = (FloatWritable) entry.getValue();
			result.append(neighbour.toString() + ":"
					+ String.format("%.3g", count.get()) + ", ");
		}
		return result.toString();
	}

}
