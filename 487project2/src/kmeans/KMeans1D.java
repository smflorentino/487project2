package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.TimeTracker;
import org.CooccurancePairs.LeftWordPartitioner;
import org.CooccurancePairs.Map;
import org.CooccurancePairs.Reduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans1D {
        
	public static enum KMEANS_COUNTER {
		NUMBER_OF_CHANGES
	};
	

 public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	 private static ArrayList<IntWritable> _centers = new ArrayList<IntWritable>();
	 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String current = value.toString();
    	IntWritable currentCenter=null;
    	int currentVal=0;
    	if(current.startsWith("c")) {
    		_centers.add(new IntWritable(Integer.parseInt(current.substring(1))));
    	}
    	else {
    		currentVal = Integer.parseInt(value.toString().substring(1));
    		int lowestDistance;
    		currentCenter = _centers.get(0);
    		lowestDistance = Math.abs(currentVal-currentCenter.get());
    		int temp;
    		for(IntWritable i : _centers) {
    			temp = Math.abs(currentVal-i.get());
    			if(temp < lowestDistance) {
    				lowestDistance = temp;
    				currentCenter = i;
    			}
    		}
    	}
    	context.write(new IntWritable(currentVal),currentCenter);

    }
    
   
 } 
 

 public static class Reduce extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

    public void reduce(TextPair key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //System.out.println("test");
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setPartitionerClass(LeftWordPartitioner.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJarByClass(KMeans1D.class);
    TimeTracker tt = new TimeTracker();
    tt.writeStartTime();
    job.waitForCompletion(true);
    tt.writeEndTime();
 }
        
}