package graphs;

import java.io.IOException;
import java.util.ArrayList;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GraphsMapper extends Mapper<LongWritable, ArrayWritable, LongWritable, ArrayWritable> {
	
	/*for node: 
	[0] - nodeId
	[1] - distance from start node
	[3] - node's adjacency list (TODO: formatted as...)
	*/
	/**Mapper for Dijkstra shortest path algorithm for graphs
	 * @author Alyssa
	 * @param nodeId - the id of the current node
	 * @param node - the metadata about the current node
	 * @return -<key,value> pair of either <nodeId, node metadata> or <neighbor's nodeId, a distance to neighbor from start>
	 */
    public void map(LongWritable nodeId, ArrayWritable node, Context context) throws IOException, InterruptedException {
    	String[] nodeArray = node.toStrings();
    	char c = nodeArray[1].charAt(0);
    	LongWritable distance = new LongWritable(Character.getNumericValue(c)); //get the distance of current node
    	//emit the current node
    	context.write(nodeId, node);
    	//parse nodeArray[2] - adjacency "list" to get an actual list
    	Long newD = distance.get() + 1;
//    	LongWritable newDistance = new LongWritable(newD);
    	for(Long neighborNodeId: this.getAdjacencyList(nodeArray[2])){
    		LongWritable lw_neighborNodeId = new LongWritable(neighborNodeId);
    		//emit <neighborNodeId, newDistance>
    		String[] d = {newD.toString()};
    		context.write(lw_neighborNodeId, new ArrayWritable(d));
    	}
    	
    }
    
    /**
     * @author Alyssa
     * @param s - a String representation of the list of nodes adjacent to the current node
     * @return - a list of the adjacent nodes
     */
    public ArrayList<Long> getAdjacencyList(String s){
    	ArrayList<Long> list = new ArrayList<Long>();
    	//TODO: implement string parsing to get adjacency list based on String format
    	return list;
    }

}
