/**
 * @author: LIN TANG
 * @since: 2017-04-10 10:59:42 AM
 * @description: the 3 airlines with the highest and lowest probability, respectively, for being on
 *					schedule; Mapper
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OnScheduleMapper extends Mapper<Object, Text, Text, Text> {
	/**
	 * on schedule airlines
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] infos = value.toString().split(",");
		//exclude the first line
		if (!"Year".equals(infos[0])) {
			String normal = "0";
			if(!"NA".equals(infos[14])){
				if (Integer.parseInt(infos[14]) <= 10 ) {
					normal = "1";
				}
			context.write(new Text(infos[8]), new Text(normal));
			}
			
		}
	}
}
