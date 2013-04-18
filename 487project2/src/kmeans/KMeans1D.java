package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans1D {
       
	public static enum KMEANS_COUNTER {
		NUMBER_OF_CHANGES
	};
	

 public static class Map extends Mapper<LongWritable, Text, VectorWritable, VectorWritable> {

	 private static ArrayList<VectorWritable> _centers = new ArrayList<VectorWritable>();
	 private static BooleanWritable _changeMade = new BooleanWritable(false);
	 private static BooleanWritable _incremented = new BooleanWritable(false);
	 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String current = value.toString();
    	VectorWritable currentCenter=null;
    	int currentVal=0;
    	if(key.get() ==0 ) {
    		//start of a new job
    	}
    	if(current.startsWith("c")) {
    		_centers.add(VectorWritable.parseVector(current));
    		//VectorWritable center = new VectorWritable(Integer.parseInt(current.substring(1)),true);
    		//_centers.add(center);
    		//context.write(center,center);
    	}
    	else {
    		currentVal = Integer.parseInt(value.toString());
    		int lowestDistance;
    		currentCenter = _centers.get(0); //get the first centroid
    		lowestDistance = Math.abs(currentVal-currentCenter.get());
    		int temp;
    		VectorWritable I;
    		for(int i=1;i<_centers.size();i++) { //resume at second centroid
    			I=_centers.get(i);
    			temp = Math.abs(currentVal-I.get());
    			if(temp < lowestDistance) {
    				lowestDistance = temp;
    				currentCenter = I;
    			}
    		}
    		VectorWritable point = new VectorWritable(currentVal);
        	context.write(currentCenter,point);
    	}
    	

    }
    
    @Override
    public void cleanup(Context context) {
    	
    	//increment once and only once for the job, if a cluster assignment changed
    	if(!_incremented.get() && _changeMade.get()) {
    		context.getCounter(KMeans1D.KMEANS_COUNTER.NUMBER_OF_CHANGES).increment(1);
    		_incremented.set(true);
    	}
    }
    
   
 } 
 

 public static class Reduce extends Reducer<VectorWritable, VectorWritable, VectorWritable, VectorWritable> {
	
    public void reduce(VectorWritable key, Iterable<VectorWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        VectorWritable center;
        ArrayList<VectorWritable> points = new ArrayList<VectorWritable>();
        int numOfPoints=0;
        System.out.println("Starting Reduce Task...");
        for (VectorWritable val : values) {
        	System.out.println("Current Key " +key.toString() + " Current Value" + val.toString());
            if(val.isCentroid()) {
            	System.out.println("Centroid Found!");
            	center = new VectorWritable(key.get(),true);
            }
            else {
            	sum+=val.get();
            	numOfPoints++;
            	points.add(new VectorWritable(val.get()));
            }
        }
        //calculate the new centroid
        center = new VectorWritable(sum/numOfPoints,true);
        context.write(center,center);
        //add all other points back to the file
        for(VectorWritable p : points) {
        	System.out.println("Current Point " + p.toString());
        	context.write(p, center);
        }
    }
    
    
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();   
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(VectorWritable.class);
    job.setOutputValueClass(VectorWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
 
    //NEED TO FIGURE OUT COMMITTERS
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJarByClass(KMeans1D.class);
   // TimeTracker tt = new TimeTracker();
   // tt.writeStartTime();
   // job.getCounters().findCounter(KMEANS_COUNTER.NUMBER_OF_CHANGES).increment(1);
    
    job.waitForCompletion(true);
    System.out.println(job.getCounters().findCounter(KMEANS_COUNTER.NUMBER_OF_CHANGES).toString());
   // tt.writeEndTime();
 }
        
}