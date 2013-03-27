package graphs;

import java.io.IOException;

import code.IntWritable;
import code.LongWritable;
import code.MapReduceBase;
import code.Mapper;
import code.OutputCollector;
import code.Reporter;
import code.Text;

public class Dijkstra {
	 public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		  
			public void map(LongWritable nodeId, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
				
			}

			  
			  
			  
			  
			  
			  
			  
			  
			  }
}
