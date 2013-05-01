package org;
/**
 * HashMapWritable Provided by: http://lintool.github.com/Cloud9/
 * TextPair Provided by: http://my.safaribooksonline.com/book/databases/hadoop/9780596521974/serialization/id3548156
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

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
//import org.apache.hadoop.util.LineReader;// HashMapWritable, used to Provide Associative Array Functionality.

/**
 * A class with a Mapper and Reducer for processing co-occurance counts using the "Stripes" method
 * @author Scott
 *
 */
public class CooccuranceStripes {

	public static class Map extends Mapper<LongWritable, Text, Text, HashMapWritable<Text,IntWritable>> {
		private final static IntWritable one = new IntWritable(1);
		private HashMapWritable<Text,IntWritable> H;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			////System.out.println(value.toString());
			String line = value.toString();
			String neighbors = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			StringTokenizer tokenizer2 = new StringTokenizer(neighbors);
			int npos=0,wpos=0;
			Text w,n;
			//keep track of words we've already processed (this to avoid over-counting pairs that have the same key/value
			//like word1,word1. For example, the file "word1 word1" represents ONE co-occurance of word1 with word1, NOT two.
			HashMap<Text, Integer> processedWords = new HashMap<Text, Integer>();
			while (tokenizer.hasMoreTokens()) {
				H=new HashMapWritable<Text, IntWritable>();
				w=new Text(tokenizer.nextToken());
				while(tokenizer2.hasMoreTokens()) {
					n=new Text(tokenizer2.nextToken());
					if(npos !=wpos) { //this is our "Neighbors" function...skip over the word we are currently on in the inner loop
						if(n.toString().equals(w.toString())) {
							if(processedWords.get(n) == null) {
								if(H.get(n) == null) {
									//we have not seen this co-occurance yet, add it to the hashmap
									H.put(n, one);
								} else {
									//add one to the value current in the Associative Array for that word
									IntWritable sum = new IntWritable( ((IntWritable) H.get(n)).get() + one.get());
									H.put(n, sum);
								}   
							}
						}
						else {
							if(H.get(n) == null) {
								//we have not seen this co-occurance yet, add it to the hashmap
								H.put(n, one);
							} else {
								//add one to the value current in the Associative Array for that word
								IntWritable sum = new IntWritable( ((IntWritable) H.get(n)).get() + one.get());
								H.put(n, sum);
							}
						}
					}

					npos++;  
				}
				npos=0;
				wpos++;
				processedWords.put(w,1);
				tokenizer2 = new StringTokenizer(neighbors);
				//emit the stripe for that particular word
				context.write(w, H);
			}
		}
	} 

	public static class TextPartitioner extends Partitioner<Text, HashMapWritable<Text,IntWritable>> {

		@Override
		public int getPartition(Text arg0, HashMapWritable<Text, IntWritable> arg1,
				int numReduceTasks) {
			return (arg0.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
			//return 0;
		}




	}
	
	/**
	 * Reduce class for Co-occurance Counts. Sum up all the stripes that have the same key (word) and emit them.
	 * @author Scott
	 *
	 */
	public static class Reduce1 extends Reducer<Text, HashMapWritable<Text, IntWritable>, Text, HashMapWritable<Text,IntWritable>> {
		private static final IntWritable one = new IntWritable(1);
		private HashMapWritable<Text,IntWritable> Hf;

		@Override
		public void reduce(Text key, Iterable<HashMapWritable<Text,IntWritable>> values, Context context) 
				throws IOException, InterruptedException {
			Hf=new HashMapWritable<Text, IntWritable>();
			int sum = 0;

			for (HashMapWritable<Text,IntWritable> val : values) {
				sum(val); //sum the associaive array
			}
			context.write(key, Hf);
		}

		
		private void sum(HashMapWritable<Text,IntWritable> H) {
			Text t;
			for(Entry<Text, IntWritable> pair : H.entrySet()) {
				t= (Text) (pair.getKey());
				int current = (int) pair.getValue().get();
				if(Hf.containsKey(t)) {
					//Text t= (Text) pair.getKey();


					IntWritable sum = new IntWritable(current+Hf.get(t).get());
					Hf.put(t, sum);
				} else {
					Hf.put(t, new IntWritable(current));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");
		job.setJarByClass(CooccuranceStripes.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HashMapWritable.class);



		job.setPartitionerClass(TextPartitioner.class);
		job.setInputFormatClass(ParagraphInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		TimeTracker tt = new TimeTracker();
		tt.writeStartTime();
		job.waitForCompletion(true);
		tt.writeEndTime();
	}

}