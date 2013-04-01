package graphs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class GraphsDriver  {
	public static void main(String args[]) throws Exception {
	 Configuration conf = new Configuration();
     
     Job job = new Job(conf, "Graphs");
     
     job.setOutputKeyClass(LongWritable.class);
     job.setOutputValueClass(ArrayWritable.class);
         
     job.setMapperClass(GraphsMapper.class);
     job.setReducerClass(GraphsReducer.class);
         
     //TODO: do we need to do this?
//     job.setInputFormatClass(ParagraphInputFormat.class);
//     job.setOutputFormatClass(TextOutputFormat.class);
         
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
     job.setJarByClass(GraphsDriver.class);
     job.waitForCompletion(true);
	}
//TODO: create initial ?list of array of strings? representing the entire graphs (1 array = 1 node)
//TODO: initially set all distances to "infinity", except for the start node itself (set to 0)
	// TODO: is infinity = 1000000 appropriate?
	
//submit MapReduce job

//check if termination condition satisfied (#nodes with distance infinity = 0, use "counter" in Hadoop)
	//TODO: figure out how to use "counter"
	
//if not done, repeat. 
}
