package graphs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import graphs.GraphRecordReader;

//public class GraphsMapper extends Mapper<LongWritable, ArrayWritable, LongWritable, ArrayWritable> {
public class GraphsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	public static enum GRAPHS_COUNTER {
		  INCOMING_GRAPHS,
		  PRUNING_BY_NCV,
		  PRUNING_BY_COUNT,
		  PRUNING_BY_ISO,
		  ISOMORPHIC
		};
	
	/*for node: 
	[0] - nodeId
	[1] - distance from start node
	[2] - node's adjacency list in format 1:2:3
	[3] - N if node not visited; Y if already visited

	*/
	/**Mapper for Dijkstra shortest path algorithm for graphs
	 * @author Alyssa
	 * @param nodeId - the id of the current node
	 * @param node - the metadata about the current node
	 * @return -<key,value> pair of either <nodeId, node metadata> or <neighbor's nodeId, a distance to neighbor from start>
	 */
//    public void map(LongWritable nodeId, ArrayWritable nodeAW, Context context) throws IOException, InterruptedException {
	    public void map(LongWritable nodeId, Text nodeAsText, Context context) throws IOException, InterruptedException {
			 String s = nodeAsText.toString();
			 String[] represents = s.split("\t");
//    	String[] nodeArray = nodeAW.toStrings();
//    	Node node = this.getNode(nodeArray);
//    	LongWritable distance = new LongWritable(node.getDistance());
			 LongWritable distance = new LongWritable(Long.parseLong(represents[1]));
    	//emit the current node and mark as visited
//    	nodeArray[3] = "Y"; //TODO: only keep if counters don't work
//    	nodeAW = new ArrayWritable(nodeArray);
//    	context.write(nodeId, nodeAW);
		context.write(nodeId, nodeAsText);
    	Long newDistance = distance.get() + 1;
		String[] d = {newDistance.toString()};
//    	for(Long neighborNodeId: node.getNeighbors()){
    	for(Long neighborNodeId: this.getNeighbors(represents[2])){
    		LongWritable lw_neighborNodeId = new LongWritable(neighborNodeId);
    		//emit <neighborNodeId, newDistance>
    		context.write(lw_neighborNodeId, new Text(newDistance.toString()));
    	}
    	//consider the node that was passed in as visited, so increment the counter    	
    	context.getCounter(GRAPHS_COUNTER.INCOMING_GRAPHS).increment(1);
    	
    }
    
    /**
     * @author Alyssa
     * @param s - a String representation of the list of nodes adjacent to the current node
     * @return - a list of the adjacent nodes
     */
    public ArrayList<Long> getAdjacencyList(String s){
    	ArrayList<Long> list = new ArrayList<Long>();
    	String neighbor="";
    	for(int i=0; i<s.length(); i++){
    		if(s.charAt(i)==':' && neighbor.length()>0){
    			list.add((long) Integer.parseInt(neighbor));
    			neighbor="";
    		}else if(s.charAt(i)!='[' && s.charAt(i)!=']'){
    			neighbor=neighbor + s.charAt(i);
    		}
    	}
    	//add final neighbor (no closing colon)
    	if(neighbor.length()>0){
    		list.add((long) Integer.parseInt(neighbor));
    	}
    	return list;
    }
    
    
    public Node getNode(String[] nodeArray){
    	long id = (long) Integer.parseInt(nodeArray[0]);
    	long distance = (long) Integer.parseInt(nodeArray[1]);
    	List<Long> neighbors = this.getAdjacencyList(nodeArray[2]);
    	return new Node(id,distance,neighbors);
    }

    public List<Long> getNeighbors(String listAsString){
    	ArrayList<Long> list = new ArrayList<Long>();
    	String[] represents = listAsString.split(":");
    	for(int i=0; i<represents.length; i++){
    		String s = represents[i];
    		s = s.trim();
    		list.add(Long.parseLong(s));
    	}
    	return list;
    }
}
