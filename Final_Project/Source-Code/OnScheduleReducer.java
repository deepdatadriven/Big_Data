/**
 * @author: LIN TANG
 * @since: 2017-04-10 10:59:42 AM
 * @description: the 3 airlines with the highest and lowest probability, respectively, for being on
 *					schedule; Reducer
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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OnScheduleReducer extends Reducer<Text, Text, Text, Text> {
	private Map<String, Double> map = new TreeMap<String, Double>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		int all =0;
		double normal = 0.0;
		while (iter.hasNext()) {
			int ss = Integer.parseInt(iter.next().toString());
			normal = normal+ss;
			all =all+1;
		}
		
		double result = normal/all;
		map.put(key.toString(), Double.valueOf(result));
	}
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(map.entrySet());  
		Collections.sort(list,new Comparator<Map.Entry<String, Double>>() {  
            // Sorted in decreasing order  
            public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {  
                return o2.getValue().compareTo(o1.getValue());  
            }  
        }); 
		context.write(new Text("highest"), new Text(""));
		for(int i= 0;i<3;i++){
			Entry<String, Double> entry = list.get(i);
			context.write(new Text(entry.getKey()), new Text(entry.getValue()+""));
		}
		context.write(new Text("lowest"), new Text(""));
		int size = list.size();
		for(int j = size - 1; j > size - 4; j--){
			Entry<String, Double> entry = list.get(j);
			context.write(new Text(entry.getKey()), new Text(entry.getValue()+""));
		}
		
		if(size==0){
			context.write(new Text("There is no value can be used, so no output."), new Text(""));
		}
	}
}
