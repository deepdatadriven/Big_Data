/**
 * @author: LIN TANG
 * @since: 2017-04-10 10:59:42 AM
 * @description: the most common reason for flight cancellations. Reducer
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CancellationsReducer extends Reducer<Text, IntWritable, Text, Text> {
	private Map<String, Integer> map = new TreeMap<String, Integer>();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		Iterator<IntWritable> iter = values.iterator();
		Integer normal = 0;
		while (iter.hasNext()) {
			int ss = Integer.parseInt(iter.next().toString());
			normal = normal + ss;
		}
		map.put(key.toString(), normal);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		if (!map.isEmpty()) {
			List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(map.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
				// Sorted in decreasing order
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}
			});
			Entry<String, Integer> entry = list.get(0);
			//A = carrier, B = weather, C = NAS, D = security
			boolean flag = false;
			if("A".equals(entry.getKey())){
				flag =true;
				context.write(new Text("Cancellation Code is A: carrier"), new Text("The count is: "+entry.getValue() + ""));
			}
			else if("B".equals(entry.getKey())){
				flag =true;
				context.write(new Text("Cancellation Code is B: weather"), new Text("The count is: "+entry.getValue() + ""));
			}
			else if("C".equals(entry.getKey())){
				flag =true;
				context.write(new Text("Cancellation Code is C: NAS"), new Text("The count is: "+entry.getValue() + ""));			
						}
			else if("D".equals(entry.getKey())){
				flag =true;
				context.write(new Text("Cancellation Code is D: security"), new Text("The count is: "+entry.getValue() + ""));
			}
			
			if(!flag){
				context.write(new Text("There is no the most common reason for flight cancellations."), new Text(""));
			}
			
		}else{
			context.write(new Text("There is no the most common reason for flight cancellations."), new Text(""));
		}
	}
}
