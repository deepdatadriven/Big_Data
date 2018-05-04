/**
 * @author: LIN TANG
 * @since: 2017-04-10 10:59:42 AM
 * @description: The 3 airports with the longest and shortest average taxi time per flight (both in and
 *					out), respectively. Mapper
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxiTimeMapper extends Mapper<Object, Text, Text, Text> {
	/**
	 * Taxi Time
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] infos = value.toString().split(",");
		// exclude the first line
		if (!"Year".equals(infos[0])) {
			if (!"NA".equals(infos[20])) {
				context.write(new Text(infos[16]), new Text(infos[20]));
			}
			if (!"NA".equals(infos[19])) {
				context.write(new Text(infos[17]), new Text(infos[19]));
			}

		}
	}
}
