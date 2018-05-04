/*
 * Author : Lin Tang
 * Class : NJIT-CS644
 * Description : Develop and test a MapReduce-based approach in your Hadoop system to find all the missing Poker cards.
 * */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
 
public class Findmissingcards extends Configured implements Tool{

    public static class FindMissingCardsMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private Text cardKey = new Text();
        private IntWritable cardValue = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {	
        	int i = 0;

        	String[] two = {"HEARTS","DIAMONDS","SPADES","CLUBS"};
        	while(i < 4){

        		cardKey.set(two[i]);
        		cardValue.set(0);
        		context.write(cardKey,cardValue);
        		i++;
        		
        	}
        	

            String singleLine = value.toString();
            String[] card = singleLine.split("\t");
            cardKey.set(card[0]);
            cardValue.set(Integer.parseInt(card[1]));
            	
           	context.write(cardKey, cardValue);
           

        }
    }

    public static class FindMissingCardsReducer extends Reducer<Text, IntWritable, Text, Text>
    {
        Text missingCard = new Text();

        public void reduce(Text token, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            ArrayList<Integer> cards = new ArrayList<Integer>();


            int total=0;
            int number=0;

            for (IntWritable val : values) {
                total+= val.get();
                number=val.get();
                cards.add(number);
            }
            StringBuilder sb = new StringBuilder();
            if(total<91){
                for (int i=1;i<=13;i++){
                    if(!cards.contains(i))
                        sb.append(i).append(";");
                }
                missingCard.set(sb.substring(0,sb.length()-1));
            }
            else{
                missingCard.set("Having all cards");
            }
            context.write(token, missingCard);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "FindMissingCards");
        job.setJarByClass(getClass());
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(FindMissingCardsMapper.class);
        job.setReducerClass(FindMissingCardsReducer.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Findmissingcards(), args);
        System.exit(result);
    }

}



