package graphs;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*for node: 
[0] - nodeId
[1] - distance from start node
[2] - node's adjacency list in format 1:2:3
[3] - N if node not visited; Y if already visited
*/
/**Reducer class for Shortest Path Problem
 * 
 * @author Alyssa
 * @param nodeId - the id of the current node
 * @param node - either node metadata or a node's distance from the start node
 * @return -<key,value> pair of <nodeId, node metadata> 
 */
public class GraphsReducer extends Reducer<LongWritable, Iterable<ArrayWritable>, LongWritable, ArrayWritable> {
	public static final long INFINITY = 1000000;//TODO: need to determine an appropriate value for infinity

	 public void reduce(LongWritable nodeId, Iterable<ArrayWritable> values, Context context) 
		      throws IOException, InterruptedException {
		 Long dmin = new Long(INFINITY); 
		 String[] node = null;
		 for(ArrayWritable array:values){
			 String[] represents = array.toStrings();
			 int d = Character.getNumericValue(represents[1].charAt(0));
			 if(this.isNode(array)){
				 node = array.toStrings();
			 }else if(d<dmin){
				 dmin = new Long(d);
			 }
			 node[1]= dmin.toString(); //change the node's distance to dmin
			 ArrayWritable nodeArray = new ArrayWritable(node);
			 context.write(nodeId, nodeArray);
		 }
		 
	 }
	 
/**Determines if an array represents a node
 * 
 * @author Alyssa
 * @param array - the array whose representation to determine
 * @return 	- true if the array represents a node
 * 			- false if the array does not represent a node (and hence represents a new distance)
 */
	private boolean isNode(ArrayWritable array) {
		if(array.toStrings().length>1){
			return true;
		}
		return false;
	}
}
