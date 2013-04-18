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
			HashMap<Text, Integer> processedWords = new HashMap<Text, Integer>();
			while (tokenizer.hasMoreTokens()) {
				H=new HashMapWritable<Text, IntWritable>();
				w=new Text(tokenizer.nextToken());
				while(tokenizer2.hasMoreTokens()) {
					n=new Text(tokenizer2.nextToken());
					//System.out.println("Entering Nested While Loop. WPOS: " + wpos + " NPOS" + npos + "N:" + n + "W: " + w + "HashMap:" + H);
					if(npos !=wpos) {
						if(n.toString().equals(w.toString())) {
							if(processedWords.get(n) == null) {
								if(H.get(n) == null) {
									//System.out.println("Adding N:" + n +"," + "1");
									H.put(n, one);
								} else {
									IntWritable sum = new IntWritable( ((IntWritable) H.get(n)).get() + one.get());
									//System.out.println("Adding N:" + n +"," + sum);
									H.put(n, sum);
								}   
							}
						}
						else {
							if(H.get(n) == null) {
								H.put(n, one);
								//System.out.println("Added One N. New Map:" + H);
							} else {
								IntWritable sum = new IntWritable( ((IntWritable) H.get(n)).get() + one.get());
								H.put(n, sum);
								//System.out.println("Added another N. New Map:"+ H);
							}
						}
					}
					
					npos++;  
				}
				npos=0;
				wpos++;
				processedWords.put(w,1);
				tokenizer2 = new StringTokenizer(neighbors);
				//System.out.println(" Mapper - Emmtting KV" + w.hashCode() + w.toString() + H.toString());
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


		/*@Override
	public int getPartition(TextPair arg0, IntWritable arg1, int numReduceTasks) {
		return (arg0.getFirst().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}*/

	}
	public static class Reduce1 extends Reducer<Text, HashMapWritable<Text, IntWritable>, Text, HashMapWritable<Text,IntWritable>> {
		private static final IntWritable one = new IntWritable(1);
		private HashMapWritable<Text,IntWritable> Hf;// = new HashMapWritable<Text, IntWritable>();

		@Override
		public void reduce(Text key, Iterable<HashMapWritable<Text,IntWritable>> values, Context context) 
				throws IOException, InterruptedException {
			Hf=new HashMapWritable<Text, IntWritable>();
			int sum = 0;

			for (HashMapWritable<Text,IntWritable> val : values) {
				sum(val); //sum(Hf, val)
			}
			//System.out.println("Reducer - Emmtting KV" + key.toString() + "  " + Hf.toString()+"\n\n");
			context.write(key, Hf);
		}

		private void sum(HashMapWritable<Text,IntWritable> H) {
			Text t;
			for(Entry<Text, IntWritable> pair : H.entrySet()) {
				//java.util.Map.Entry<Text, IntWritable> pair = (java.util.Map.Entry<Text, IntWritable>) it.next();
				t= (Text) (pair.getKey());
				//System.out.println("Currently Processing..............: "+ t.toString());
				int current = (int) pair.getValue().get();
				if(Hf.containsKey(t)) {
					//Text t= (Text) pair.getKey();
					

					IntWritable sum = new IntWritable(current+Hf.get(t).get());
					Hf.put(t, sum);
					//System.out.println("Item Found! New Count: " + current + "word: " + t.toString());
				} else {
					//System.out.println("Item Found for the First Time - word: " + t.toString());
					Hf.put(t, new IntWritable(current));
				}
				//System.out.println("Done with that word. HashMap: " + Hf.tString());
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