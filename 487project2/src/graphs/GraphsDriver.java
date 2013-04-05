package graphs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class GraphsDriver  {
	public static enum GRAPHS_COUNTER {
		  INCOMING_GRAPHS,
		  PRUNING_BY_NCV,
		  PRUNING_BY_COUNT,
		  PRUNING_BY_ISO,
		  ISOMORPHIC
		};
	
	public static void main(String args[]) throws Exception {
		//TODO: how to pass input to mapper as 
		//TODO: determine the number of nodes in graph by parsing file
		long numNodes=0;
//		do{
			 Configuration conf = new Configuration();
		     
		     Job job = new Job(conf, "Graphs");
		     
		     
		     
		     job.setOutputKeyClass(LongWritable.class);
		     job.setOutputValueClass(ArrayWritable.class);
		         
		     job.setMapperClass(GraphsMapper.class);
		     job.setReducerClass(GraphsReducer.class);

		     FileInputFormat.addInputPath(job, new Path("graphsData/testInput.txt"));
		     FileOutputFormat.setOutputPath(job, new Path("graphsData/testOutput.txt"));

		     
//		     TODO: return to args after testing
//		     FileInputFormat.addInputPath(job, new Path(args[0]));
//		     FileOutputFormat.setOutputPath(job, new Path(args[1]));
		     
		     job.setJarByClass(GraphsDriver.class);
		     job.waitForCompletion(true);
		     
		     Counters counters = job.getCounters();
		     Counter c1 = counters.findCounter(GRAPHS_COUNTER.INCOMING_GRAPHS);
	//	}while(c1.getValue()<numNodes);
    	 
     
	}
//TODO: create initial ?list of array of strings? representing the entire graphs (1 array = 1 node)
//TODO: initially set all distances to "infinity", except for the start node itself (set to 0)
	// TODO: is infinity = 1000000 appropriate?
	
//submit MapReduce job

//check if termination condition satisfied (#nodes with distance infinity = 0, use "counter" in Hadoop)
	//TODO: figure out how to use "counter"
	
//if not done, repeat. 
	
	
}
