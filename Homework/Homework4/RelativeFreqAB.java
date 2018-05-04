import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class RelativeFreqAB {
	
    //Map AB and A
    //Key: AB or A   Value: 1
	public static class wordFreqMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			//replace all symbols to "" except a-z " " _ +
			String line = value.toString().trim().toLowerCase().replaceAll("[^a-z _+]", "");
			
			StringTokenizer token = new StringTokenizer(line," ");
			List<String> words = new ArrayList<String>();
			
			while(token.hasMoreElements()){

				String wordAB = new String(token.nextToken());
				wordAB = wordAB.replaceAll("\\t", "");
				wordAB = wordAB.trim();
  		    	if( wordAB. length() < 2 ) continue;
				words.add(wordAB);

			}
			//A 1 or A B 1
			for (int i = 1; i < words.size(); i++){
				
        		if (i == 1){//get the first word
        			context.write(new Text("!"+" "+words.get(i-1).trim()), new LongWritable(1));
        			context.write(new Text(words.get(i-1).trim()+" "+words.get(i).trim()), new LongWritable(1));
        			context.write(new Text("!"+" "+words.get(i).trim()), new LongWritable(1));
        		}
        		else{
					context.write(new Text(words.get(i-1).trim()+" "+words.get(i).trim()), new LongWritable(1));
        			context.write(new Text("!"+" "+words.get(i).trim()), new LongWritable(1));
        		}
			}
		
		}

	}
	
	//Key: AB  Value: Count of AB
	//OR Key: A  Value: Count of A
	public static class wordFreqReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		
		public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{

			int count = 0;
        	// calculate count of AB or A
			for (LongWritable val : values){
				count += val.get();
			}

			context.write(key, new LongWritable(count));
		}
 	}
  	// Key: A B Value: count of AB 
  	//OR Key: A  Value: count of A
	public static class pairFreqMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{

			String line = value.toString().trim();
      		String word = line.split("\\t")[0];//get word
      		int count = (int)Integer.parseInt(line.split("\\t")[1]);// count of A
      		context.write(new Text(word) ,new LongWritable(count));	
		}

	}
	//Put A and it's count to the hashmap and this pair of key-value will be abandoned
	//Key: A B   Value: count of AB+ " " + count of A
	public static class pairFreqReducer extends Reducer<Text,LongWritable,Text,Text>{

    	HashMap<String,Integer> aOccur = new HashMap<String,Integer>();

		public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{
			//"!" is the smallest one in ascii, so it will be on the top after being sorted by the mapreduce
      		if (key.toString().trim().contains("!")){

        		for( LongWritable val : values){

          			int countA = (int)Integer.parseInt(val.toString().trim());
          			String wordA = key.toString().trim().split(" ")[1];

         			if (aOccur.containsKey(wordA)){
				    	continue;
			    	}
					else{//put A and the count of A into hashmap
				    aOccur.put(wordA, countA);
			    	}

        		}
        		return;//this key-value will be abandoned 
      		}
      
      		for(LongWritable val : values){//put the count of A into value

          		String wordA = key.toString().trim().split(" ")[0];
          		int countA = aOccur.get(wordA);
          		context.write(key, new Text(val+" "+countA));

      		}
    	}
  	}
    // Key: count of AB Value: A B count of A
	public static class topPairMapper extends Mapper<LongWritable,Text,LongWritable,Text>{

		public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{

      		String line = value.toString().trim();
      		String word = line.split("\\t")[0];//get AB
     		int countAB = (int)Integer.parseInt(line.split("\\t")[1].split(" ")[0]);// count of AB
      		int countA = (int)Integer.parseInt(line.split("\\t")[1].split(" ")[1]);// count of A
      		context.write(new LongWritable(countAB) ,new Text(word+" "+countA));

		}

	}
	//select the top 1000 according to the count of AB
	public static class topPairReducer extends Reducer<LongWritable,Text,LongWritable,Text>{

    	static int kTopCount = 1000;//Top K

		public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
   
      		for(Text val : values){

				if (kTopCount > 0){
					context.write(key, val);
					kTopCount = kTopCount - 1;
				}
				else{
					break;
				}

			}
    	} 
  	}

	//Key: f(B|A)  Value: AB
    //calculate f(B|A)
    //Relative frequency of A and B is countAB/countA
    //count(A,B) is the number of times A and B co-occur in a document
    //count(A) the number of times A occurs with anything else
   public static class RelFreqMapper extends Mapper<LongWritable,Text,FloatWritable,Text>{

		public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
	    //format of line: A+" "+B+"\\t+count of AB
			String line = value.toString().trim();

     		try{

				String wordA = line.split("\\t")[1].split(" ")[0];//get word A
				String wordB = line.split("\\t")[1].split(" ")[1];//get word B
				float countAB = (float)Integer.parseInt(line.split("\\t")[0]);//get count of AB
				float countA = (float)Integer.parseInt(line.split("\\t")[1].split(" ")[2]) - countAB;//countA
				float relativeFreq = 0;
        		//calculate f(B|A)
        		if (countA != 0){
					relativeFreq = countAB/countA;
       			}

        		context.write(new FloatWritable(relativeFreq), new Text(wordA+" "+wordB));	
			}
			catch(Exception e){
				e.printStackTrace();
			}

		}
	}
	//select the top 100 according to the f(B|A) value
	public static class RelFreqReducer extends Reducer<FloatWritable,Text,FloatWritable,Text>{

		static int kTop = 100;//Top K

		public void reduce(FloatWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{

			for(Text val : values){
				if (kTop > 0){
					context.write(key, val);
					kTop = kTop - 1;
				}
				else{
					break;
				}

			}
		}
	}
    // Sorting the top 1000
	public static class SumWritableComparator extends WritableComparator{
	
		protected SumWritableComparator(){
			super(LongWritable.class,true);
		}
	
		@SuppressWarnings("datatypes")
		@Override
		public int compare(WritableComparable pair1, WritableComparable pair2){

			LongWritable pairF1=(LongWritable)pair1;
			LongWritable pairF2=(LongWritable)pair2;
		
			return -1*pairF1.compareTo(pairF2);
		}

	}
 	//sorting the top 100
 	public static class TopWritableComparator extends WritableComparator{
	
		protected TopWritableComparator(){
			super(FloatWritable.class,true);
		}
	
		@SuppressWarnings("datatypes")
		@Override
		public int compare(WritableComparable pair1, WritableComparable pair2){

			FloatWritable pairF1=(FloatWritable)pair1;
			FloatWritable pairF2=(FloatWritable)pair2;
		
			return -1*pairF1.compareTo(pairF2);
		}

	}
 
	public static void main(String []args)throws Exception{
		
		Configuration conf=new Configuration();
		Job wordFreq=new Job(conf,"Times A B Co-occur and count of A occurrence in the Document");
		wordFreq.setJarByClass(RelativeFreqAB.class);
		FileInputFormat.addInputPath(wordFreq, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordFreq,new Path("TEMP1"));
		wordFreq.setMapperClass(wordFreqMapper.class);
		wordFreq.setReducerClass(wordFreqReducer.class);
		wordFreq.setMapOutputKeyClass(Text.class);
		wordFreq.setMapOutputValueClass(LongWritable.class);
		wordFreq.setOutputKeyClass(Text.class);
		wordFreq.setOutputValueClass(LongWritable.class);
		wordFreq.waitForCompletion(true);
		
		Job pairFreq=new Job(conf,"Count of A and B");
		pairFreq.setJarByClass(RelativeFreqAB.class);
		FileInputFormat.addInputPath(pairFreq, new Path("TEMP1"));
		FileOutputFormat.setOutputPath(pairFreq,new Path("TEMP2"));
		pairFreq.setMapperClass(pairFreqMapper.class);
		pairFreq.setReducerClass(pairFreqReducer.class);
		pairFreq.setMapOutputKeyClass(Text.class);
		pairFreq.setMapOutputValueClass(LongWritable.class);
		pairFreq.setOutputKeyClass(Text.class);
		pairFreq.setOutputValueClass(Text.class);
		pairFreq.waitForCompletion(true);
   
   		Job topPair=new Job(conf,"Top 1000 of count of A and B");
		topPair.setJarByClass(RelativeFreqAB.class);
		FileInputFormat.addInputPath(topPair, new Path("TEMP2"));
		FileOutputFormat.setOutputPath(topPair,new Path("TEMP3"));
		topPair.setMapperClass(topPairMapper.class);
		topPair.setReducerClass(topPairReducer.class);
		topPair.setMapOutputKeyClass(LongWritable.class);
		topPair.setMapOutputValueClass(Text.class);
        topPair.setSortComparatorClass(SumWritableComparator.class);
		topPair.setOutputKeyClass(LongWritable.class);
		topPair.setOutputValueClass(Text.class);
		topPair.waitForCompletion(true);
   
   		Job RelFreq=new Job(conf,"Top 100 Relative-Frequency of A and B");
		RelFreq.setJarByClass(RelativeFreqAB.class);
		FileInputFormat.addInputPath(RelFreq, new Path("TEMP3"));
		FileOutputFormat.setOutputPath(RelFreq,new Path(args[1]));
		RelFreq.setMapperClass(RelFreqMapper.class);
		RelFreq.setReducerClass(RelFreqReducer.class);
		RelFreq.setMapOutputKeyClass(FloatWritable.class);
		RelFreq.setMapOutputValueClass(Text.class);
        RelFreq.setSortComparatorClass(TopWritableComparator.class);
		RelFreq.setOutputKeyClass(FloatWritable.class);
		RelFreq.setOutputValueClass(Text.class);
		System.exit(RelFreq.waitForCompletion(true) ? 0 : 1);
		
	}
	
}


