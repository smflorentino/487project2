package org;
        
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
//import java.util.Map.Entry;
        
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
//import org.apache.hadoop.util.LineReader;

//import archive.TextPair;
//import archive.RelativeFrequencyPairs.LeftWordPartitioner;
        
public class RelativeFrequencyPairs {
        
	public static class Map extends Mapper<LongWritable, Text, TextPair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static String STAR = "*";
		//private final static IntWritable none = new IntWritable(-1);
		private TextPair word = new TextPair();
		private String w;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String n="";
			HashMap<String, Integer> hm = new HashMap<String, Integer>();
			//System.out.println(value.toString());
			String line = value.toString();
			String neighbors = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			StringTokenizer tokenizer2 = new StringTokenizer(neighbors);
			int wpos=0;
			int npos=0;
			HashMap<String, Integer> processedWords = new HashMap<String, Integer>();
			while(tokenizer.hasMoreTokens()) {
				w=tokenizer.nextToken();
				
				//In mapper combining
				if(hm.containsKey(w)) {
	        		hm.put(w, hm.get(w)+1);
	        	}
	        	else {
	        		hm.put(w,1);
	        	}
				
				//end in mapper combining
				
				
				while(tokenizer2.hasMoreTokens()) {
					n=tokenizer2.nextToken();
				//	System.out.println("Entering Nested While Loop. WPOS: " + wpos + " NPOS" + npos + "N:" + n + "W: " + w);
					if(npos!=wpos) { //this is our WORKING "neighbors" function
						if(n.equals(w)) {
							if(processedWords.get(n) == null) {
					//			System.out.println("Adding N:" + n +"," + "1");
								//System.out.println("Emitting KV Pair: "+ w + "," + n);
								context.write(new TextPair(w,n),one);
								context.write(new TextPair(w,STAR), one);
							}
						} else {
						//	System.out.println("Emitting KV Pair: "+ w + "," + n);
							context.write(new TextPair(w,n),one);
							context.write(new TextPair(w,STAR), one);
						}
					}
					npos++;
				}
				processedWords.put(w, 1);
				npos=0;
				wpos++;
				tokenizer2=new StringTokenizer(neighbors);
			}
			
			//emit special pairs
			/*
			java.util.Map.Entry<String, Integer> pair;
	    	Iterator<Entry<String, Integer>> it = hm.entrySet().iterator();
	    	Text t; IntWritable i;
	    	while(it.hasNext()) {
	    		pair=it.next();
	    		t=new Text(pair.getKey());
	    		i = new IntWritable(pair.getValue());
	    		System.out.println("Emitting KV Pair: "+ t + ",* I:" + i);
	    		context.write(new TextPair(t, new Text("*")), i);
	    	}*/
	    	//end emit special map pairs

		}


	} 

	public static class TextPairComparator extends WritableComparator {
	    private static final String SPECIAL = "*";
	    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
	    
	    public TextPairComparator() {
	      super(TextPair.class,true);
	    }
	    
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	TextPair a = (TextPair) w1;
	    	TextPair b = (TextPair) w2;
	    	return a.getFirst().compareTo(b.getFirst());
	    }
	    
	    /*@Override
	    public int compare(WritableComparable a, WritableComparable b) {
	      if (a instanceof TextPair && b instanceof TextPair) {
	    	  TextPair A = (TextPair) a; TextPair B = (TextPair) b;
	    	  if(A.getFirst().compareTo(B.getFirst()) == 0) {
	    		  if(A.getSecond().equals(SPECIAL)) {
	    			  return -1;
	    		  }
	    		  else {
	    			  return 1;
	    		  }
	    	  } else {
	    		  return A.compareTo(B);
	    	  }
	    	  
	    	  
	        //return ((TextPair) a).first.compareTo(((TextPair) b).first);
	      }
	      return super.compare(a, b)
	    }*/
	    
	  }
	
public static class LeftWordPartitioner extends Partitioner<TextPair, IntWritable> {
	private static final char SPECIAL = '*';
	public LeftWordPartitioner() {}
	@Override
	public int getPartition(TextPair key, IntWritable value, int numReduceTasks) {
		TextPair arg0 = key;
		Text sec = new Text(key.getFirst().toString());
		/*if(key.getSecond().charAt(0)== SPECIAL) {
			
			//System.out.println("*Partitioning...:" + arg0.getFirst() + "," + arg0.getSecond() + " " + sec.hashCode());
			
			return (arg0.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}*/
		//System.out.println("Partitioning...:" + arg0.getFirst() + "," + arg0.getSecond() + " "+ sec.hashCode());
		return (sec.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
	 
 }
 public static class Reduce extends Reducer<TextPair, IntWritable, TextPair, FloatWritable> {
	 private static final char SPECIAL = '*';
	 private int _marginal = 0;
	 private int _sum=0;
	 
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
       //System.out.print("\nReducer Key: " + key.getFirst() + "," + key.getSecond());
       //System.out.println("\nMarginal: " + _marginal + " Sum: " + _sum);
        if(key.getSecond().toString().charAt(0) == SPECIAL) {
        	_sum=0;
        	_marginal=0;
        	for (IntWritable val : values) {
                
        		//System.out.print(" " + val.toString() + " ");
        		_marginal += val.get();
            }  
        	//System.out.println("-");
        } else {
        	for (IntWritable val : values) {
        		//System.out.print(" " + val.toString() + " ");
                _sum += val.get();
            }
          //  System.out.println("test");
            context.write(key, new FloatWritable( (float) _sum /_marginal));
            //_marginal =0;
           _sum=0;
        }

    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    //output of mappers
     job.setMapOutputKeyClass(TextPair.class);
     job.setMapOutputValueClass(IntWritable.class);
    //job.setGroupingComparatorClass(cls)
     // job.setSortComparatorClass(TextPairComparator.class);
     //output of reducers
  //  job.setSortComparatorClass(TextPairComparator.class);
    //job.setSortComparatorClass(TextPairComparator.class);
    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setPartitionerClass(LeftWordPartitioner.class);
    
    job.setNumReduceTasks(4);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(ParagraphInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJarByClass(RelativeFrequencyPairs.class);
    TimeTracker tt = new TimeTracker();
    tt.writeStartTime();
    job.waitForCompletion(true);
    tt.writeEndTime();
 }
        
}

