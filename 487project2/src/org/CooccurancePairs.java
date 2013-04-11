package org;
//4-10-2013
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
        
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
        
public class CooccurancePairs {
        
 public static class Map extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private TextPair word = new TextPair();
    private String w;
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String n="";
    	//System.out.println(value.toString));
        String line = value.toString();
        String neighbors = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        StringTokenizer tokenizer2 = new StringTokenizer(neighbors);
        int wpos=0;
        int npos=0;
        HashMap<String, Integer> processedWords = new HashMap<String, Integer>();
        while(tokenizer.hasMoreTokens()) {
    	w=tokenizer.nextToken();
    	while(tokenizer2.hasMoreTokens()) {
    		n=tokenizer2.nextToken();
    		//System.out.println("Entering Nested While Loop. WPOS: " + wpos + " NPOS" + npos + "N:" + n + "W: " + w);
    		if(npos!=wpos) { //this is our WORKING "neighbors" function
    			if(n.equals(w)) {
    				if(processedWords.get(n) == null) {
    					//System.out.println("Adding N:" + n +"," + "1");
    					context.write(new TextPair(w,n),one);
    				}
    			} else {
    				context.write(new TextPair(w,n),one);
    			}
    		}
    		npos++;
    	}
    	processedWords.put(w, 1);
    	npos=0;
    	wpos++;
    	tokenizer2=new StringTokenizer(neighbors);
    }
        
        
        /*while(tokenizer.hasMoreTokens()) {
        	w=tokenizer.nextToken();
        	while(tokenizer2.hasMoreTokens()) {
        		n=tokenizer2.nextToken();
        		System.out.println("Entering Nested While Loop. WPOS: " + wpos + " NPOS" + npos + "N:" + n + "W: " + w);
        		if(npos!=wpos) { //this is our "neighbors" function
        			if(wpos >0 && !(w.equals(n))) {
        				context.write(new TextPair(w,n),one);
        				System.out.println("if1");//"WPOS: " + wpos + " NPOS: " + npos + w.toString() + "," + n.toString());
        			}
        			else if(wpos==0) {
        				context.write(new TextPair(w,n), one);
        				System.out.println("if2");//"WPOS: " + wpos + " NPOS: " + npos + w.toString() + "," + n.toString());
            		}
        		}
        		npos++;
        	}
        	npos=0;
        	wpos++;
        	tokenizer2=new StringTokenizer(neighbors);
        }*/
        
        /*boolean foundItself=false;
         
        while (tokenizer.hasMoreTokens()) {
        	w=tokenizer.nextToken();
           while(tokenizer2.hasMoreTokens()) {
        	   n=tokenizer2.nextToken();
        	   if(w.equals(n)) {
        		   if(!foundItself) {
        			   foundItself=true;
        		   } else {
        			   word= new TextPair(w,n);
        			   context.write(word, one);
        		   }
        	   } 
        	   else {
        		   word = new TextPair(w, n);
            	   context.write(word, one);
        	   }
        	   
           }
           tokenizer2= new StringTokenizer(neighbors);
           foundItself=false;
        }*/
    }
    
   
 } 
 
public static class LeftWordPartitioner extends Partitioner<TextPair, IntWritable> {

	public LeftWordPartitioner() {}
	@Override
	public int getPartition(TextPair arg0, IntWritable arg1, int numReduceTasks) {
		return (arg0.getFirst().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
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
    job.setInputFormatClass(ParagraphInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJarByClass(CooccurancePairs.class);
    TimeTracker tt = new TimeTracker();
    tt.writeStartTime();
    job.waitForCompletion(true);
    tt.writeEndTime();
 }
        
}






















