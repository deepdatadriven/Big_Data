/**
 * @author: LIN TANG
 * @since: 2017-04-10 10:59:42 AM
 * @description: the most common reason for flight cancellations. Mapper
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CancellationsMapper extends Mapper<Object, Text, Text, IntWritable> {
	/**
	 * Cancellation reasons
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] infos = value.toString().split(",");
		// exclude the first line
		if (!"Year".equals(infos[0])) {
			if ("1".equals(infos[21])&&!"NA".equals(infos[22])&&infos[22].trim().length()>0) {
				context.write(new Text(infos[22]), new IntWritable(1));
			}

		}
	}
}
