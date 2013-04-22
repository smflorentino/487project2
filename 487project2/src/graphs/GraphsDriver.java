package graphs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
		//TODO: calculate the number of nodes in graph by parsing file
		long numNodes=50;
		Counter c1;
		long currentIteration = 1;
	     Path inputPath = new Path("/*.*");
	     Path outputPath = new Path("/graphsData");
		do{
			 Configuration conf = new Configuration();
		     if(currentIteration != 1){
		     //************ Scott's iterative code *********************************//
		   //delete the old input directory, and re-create it
				FileSystem.get(conf).delete(inputPath, true);
				FileSystem.get(conf).mkdirs(inputPath);
			//copy the output from the last MR job
			//	inputPath = new Path("/part-r-00000");
				FileUtil.copy(FileSystem.get(conf), new Path("/graphsData" + "/part-r-00000"), FileSystem.get(conf), inputPath, true, conf);
			//delete the output directory from the last job
				FileSystem.get(conf).delete(outputPath, true);
			//********************end Scott's iterative code ***************************//
		     }
		     Job job = new Job(conf, "Graphs");
			inputPath = new Path("/part-r-00000");     		     
		     job.setOutputKeyClass(LongWritable.class);
//		     job.setOutputValueClass(ArrayWritable.class);
		     job.setOutputValueClass(Text.class);
		         
		     job.setMapperClass(GraphsMapper.class);
		     job.setReducerClass(GraphsReducer.class);
		     


		     FileInputFormat.addInputPath(job, inputPath);
		     FileOutputFormat.setOutputPath(job, outputPath);

		     
//		     TODO: return to args after testing
//		     FileInputFormat.addInputPath(job, new Path(args[0]));
//		     FileOutputFormat.setOutputPath(job, new Path(args[1]));
		     
		     job.setJarByClass(GraphsDriver.class);
		     job.waitForCompletion(true);

		     //set # reducers to number of nodes
		     
		     
		     Counters counters = job.getCounters();
//		     c1 = counters.findCounter(GRAPHS_COUNTER.INCOMING_GRAPHS);
//		     System.out.println("Counter value at end: "+c1.getValue());
		     currentIteration++;
		     System.out.println("Current iteration: "+currentIteration);
		}while(currentIteration<numNodes);
    	 
     
	}
	
//submit MapReduce job

//check if termination condition satisfied (#nodes with distance infinity = 0, use "counter" in Hadoop)
	//TODO: figure out how to use "counter"
	
//if not done, repeat. 
	public long getNumNodes(String inputFileName){
		return 0;
		
	}
	
}
