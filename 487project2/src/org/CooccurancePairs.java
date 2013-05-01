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

/**
 * Map, Reduce, and Partition Operations for computing Co-occurance Word Counts using Pairs
 * @author Scott
 *
 */
public class CooccurancePairs {

	public static class Map extends Mapper<LongWritable, Text, TextPair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private TextPair word = new TextPair();
		private String w;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String n="";
			//create two strings from the input, we will iterate through both
			String line = value.toString();
			String neighbors = value.toString();
			//tokenize the input
			StringTokenizer tokenizer = new StringTokenizer(line);
			StringTokenizer tokenizer2 = new StringTokenizer(neighbors);
			int wpos=0;
			int npos=0;
			//keep track of words we've already processed (this to avoid over-counting pairs that have the same key/value
			//like word1,word1. For example, the file "word1 word1" represents ONE co-occurance of word1 with word1, NOT two.
			HashMap<String, Integer> processedWords = new HashMap<String, Integer>();
			while(tokenizer.hasMoreTokens()) {
				w=tokenizer.nextToken();
				while(tokenizer2.hasMoreTokens()) {
					n=tokenizer2.nextToken();
					if(npos!=wpos) { //this is our WORKING "neighbors" function...process all other words but the one wer're on
						if(n.equals(w)) {
							if(processedWords.get(n) == null) {
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
				//reset the inner tokenizer for the next word in the outer tokenizer
				tokenizer2=new StringTokenizer(neighbors);
			}

		}


	} 

	/**
	 * The Pairs method requires that all of the left words in the Word Pair go to the same reducer...that is accomplished here
	 * @author Scott
	 *
	 */
	public static class LeftWordPartitioner extends Partitioner<TextPair, IntWritable> {

		public LeftWordPartitioner() {}
		@Override
		public int getPartition(TextPair arg0, IntWritable arg1, int numReduceTasks) {
			return (arg0.getFirst().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}

	}
	
	/**
	 * Simply sum all of the keys in the reducer
	 * @author Scott
	 *
	 */
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
		//set out custom input formats and Partitioners here
		job.setPartitionerClass(LeftWordPartitioner.class);
		job.setInputFormatClass(ParagraphInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(CooccurancePairs.class);
		//keep track of runtime on CCR
		TimeTracker tt = new TimeTracker();
		tt.writeStartTime();
		job.waitForCompletion(true);
		tt.writeEndTime();
	}

}






















