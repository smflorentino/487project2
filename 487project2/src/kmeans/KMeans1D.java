package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.nio.file.FileSystem;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;

public class KMeans1D {
       
	
	public static final int NUM_OF_CENTROIDS = 2;
	public static enum KMEANS_COUNTER {
		NUMBER_OF_CHANGES,
		NUMBER_OF_CLUSTERFILES_CREATED
	};
	

 public static class Map extends Mapper<LongWritable, Text, VectorWritable, VectorWritable> {

	 private ArrayList<VectorWritable> _centers = new ArrayList<VectorWritable>();
	 private static BooleanWritable _changeMade = new BooleanWritable(false);
	 private static BooleanWritable _incremented = new BooleanWritable(false);
	 
	 private ArrayList<VectorWritable> _points;
	 
	 @Override
	 public void setup(Context context) {
		 //read in the clusters file
		 // Path path = new Path("hdfs://localhost)
		 Configuration config = context.getConfiguration();
		 int tries = 0;

		 while(tries <10) {
			 try {
				 FileSystem dfs = FileSystem.get(config);
				 Path s = new Path("/centers/centers.txt");
				 
				 FSDataInputStream fs = dfs.open(s);
				 BufferedReader reader = new BufferedReader( new InputStreamReader(fs));
				 String str = reader.readLine();
				 while(str != null) {
					 System.out.println(str);
					 _centers.add(VectorWritable.parseCentroid(str));
					 str = reader.readLine();
				 }
				 reader.close();
				 //success. exit the method.
				 return;

			 } catch (IOException e) {
				 //something happening. try again.
				 System.err.println("Cannot get the DFS. Trying again...");
				 tries++;
				 try {
					 Thread.sleep(1000);
				 } catch (InterruptedException e1) {
					 // TODO Auto-generated catch block
					 e1.printStackTrace();
				 }
				 e.printStackTrace();
			 }

		 }



	 }
	 
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	VectorWritable point; 
    	String current = value.toString();
    	VectorWritable currentCenter=null;
    	int currentVal=0;
    	if(key.get() ==0 ) {
    		//start of a new job
    	}
    	
    	//read the datapoint.
    	point = VectorWritable.parseVector(current);
    	//_points.add(VectorWritable.parseVector(current));
    
    	
    	//we (hopefully) have all the centers, let's calculate
    	//TODO: add global checking for multiple mappers and sleep accordingly
    	
    		//TODO get the centroid from the vector itself
    		currentVal = point.get();
    		int lowestDistance;
    		currentCenter = _centers.get(0); //get the first centroid
    		lowestDistance = Math.abs(currentVal-currentCenter.get());
    		int temp;
    		VectorWritable I;
    		for(int i=1;i<_centers.size();i++) { //resume at second centroid
    			I=_centers.get(i);
    			temp = Math.abs(currentVal-I.get());
    			System.out.println("Current Center: " + currentCenter.toString() + " Temp:" + temp);
    			if(temp < lowestDistance) {
    				//_incremented.set(true); //now we have to continue, since a cluster assignment changed
    				lowestDistance = temp;
    				currentCenter = I;
    			}
    		}
    		
    		//check if cluster assignment changed
    		System.out.println("checking: point centroid: " + point.getCentroid() + " against centroid: " + currentCenter.toString());
    		if(point.setCentroid(currentCenter)) {
    			_incremented.set(true);
    		}
    		
        	context.write(currentCenter,point);
    	 
    	

    }
    
    @Override
    public void cleanup(Context context) {
    	System.out.println("Entering Cleanup");
    	//increment once and only once for the job, if a cluster assignment changed
    	if(_incremented.get() && !_changeMade.get()) {
    		System.out.println("ATTEMPTING TO INCREMENT COUNTER");
    		context.getCounter(KMeans1D.KMEANS_COUNTER.NUMBER_OF_CHANGES).increment(1);
    		_incremented.set(true);
    		_changeMade.set(true);
    	}
    }
    
   
 } 
 

 public static class Reduce extends Reducer<VectorWritable, VectorWritable, VectorWritable, VectorWritable> {

	ArrayList<VectorWritable> _centers = new ArrayList<VectorWritable>();
	
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
            	//this is no longer needed
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
        
        //TODO change this
        _centers.add(center);
       // context.write(center,center);
        //add all other points back to the file
        for(VectorWritable p : points) {
        	System.out.println("Current Point " + p.toString());
        	context.write(p, center);
        }
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
    	//write all the new centers back to the clusters file
    	//append the counter to our file name so as not to overwrite anything
    	context.getCounter(KMeans1D.KMEANS_COUNTER.NUMBER_OF_CLUSTERFILES_CREATED).increment(1);
    	int fileNumber = (int) context.getCounter(KMeans1D.KMEANS_COUNTER.NUMBER_OF_CLUSTERFILES_CREATED).getValue();

    	Configuration config = context.getConfiguration();
    	FileSystem dfs = FileSystem.get(config);
    	Path s = new Path("/centersoutputs/centers" + fileNumber + ".txt");
    	//dfs.createNewFile(s);
    	//SequenceFile.Writer writer = SequenceFile.createWriter(dfs, config, s, VectorWritable.class, VectorWritable.class);
    	//writer.append(key, val)
    	FSDataOutputStream fs = dfs.create(s);//dfs.open(s);
    	
    	//write each of the centroids back to a centers file
    	for(VectorWritable center : _centers) {
    		//writer.append(center,center);
    		fs.writeBytes(center.toString());
    		fs.writeBytes("\n");
    	}
    	//writer.close();
    	fs.close();
    	//success. exit the method.
    	return;


    }
 }
//asd
 
 public static void main(String[] args) throws Exception {
	Path inputPath = new Path(args[0]);
	Path outputPath = new Path(args[1]);
	int counter = 0;
	int internalCounter=0;
	Configuration conf = new Configuration();   
    Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(VectorWritable.class);
    job.setOutputValueClass(VectorWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
 
    //NEED TO FIGURE OUT COMMITTERS
    
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(KMeans1D.class);
   // TimeTracker tt = new TimeTracker();
   // tt.writeStartTime();
   // job.getCounters().findCounter(KMEANS_COUNTER.NUMBER_OF_CHANGES).increment(1);
    
    job.waitForCompletion(true);
    
    while(job.getCounters().findCounter(KMEANS_COUNTER.NUMBER_OF_CHANGES).getValue() > counter) {

    	
    	internalCounter++;
    	counter =0;
    	System.out.println("****************************\nStarting Iteration + " + internalCounter + "\n****************************");
    	
    	//print out stats from the job that just completed
    	 Path s = new Path(args[1]+"/part-r-00000");
		 FSDataInputStream fs = FileSystem.get(conf).open(s);
		 BufferedReader reader = new BufferedReader( new InputStreamReader(fs));
		 String str = reader.readLine();
		 while(str != null) {
			 System.out.println(str);
			 str = reader.readLine();
		 }
    	reader.close();
    	
    	
    	//clean up from last job
    	
    	//delete the old input directory, and re-create it
    	FileSystem.get(conf).delete(inputPath, true);
    	FileSystem.get(conf).mkdirs(inputPath);
    	//FileSystem.get(conf).create(inputPath);
    	
    	//copy the output from the last MR job
    	//FileUtil.co
    	FileUtil.copy(FileSystem.get(conf), new Path(args[1] + "/part-r-00000"), FileSystem.get(conf), new Path(args[0]+ "/part-r-00000"), true, conf);
    	
  
    	//delete the output directory from the last job
    	FileSystem.get(conf).delete(outputPath, true);
    	
    	//delete the original centers file
        FileSystem.get(conf).delete(new Path("/centers/centers.txt"),false);
        
        //merge all of the center files from each reducer, put it back into a new centers.txt file
        //org.apache.hadoop.fs.FileUtil.copyMerge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource, Configuration conf, String addString);
        FileUtil.copyMerge(FileSystem.get(conf), new Path("/centersoutputs"), FileSystem.get(conf), new Path("/centers/centers.txt"), true, conf,"");
        
        System.out.println(job.getCounters().findCounter( KMEANS_COUNTER.NUMBER_OF_CHANGES).getValue()+ "KMEANS COUNTER");
        
        
        //start the new job
        conf = new Configuration();   
        job = new Job(conf, "wordcount");
        
        job.setOutputKeyClass(VectorWritable.class);
        job.setOutputValueClass(VectorWritable.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
     
        //NEED TO FIGURE OUT COMMITTERS
        
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setJarByClass(KMeans1D.class);
        
        job.waitForCompletion(true);
    }
     }
        
}