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
//public class GraphsReducer extends Reducer<LongWritable, Iterable<ArrayWritable>, LongWritable, ArrayWritable> {
public class GraphsReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	public static final long INFINITY = 1000000;//TODO: need to determine an appropriate value for infinity

//	 public void reduce(LongWritable nodeId, Iterable<ArrayWritable> values, Context context) 
//		      throws IOException, InterruptedException {
	public void reduce(LongWritable nodeId, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		 Long dmin = new Long(INFINITY); 
		 String node = "";
//		 for(ArrayWritable array:values){
		 String[] represents = null;
		 for(Text nodeAsText:values){
			 String s = nodeAsText.toString();
//			  String[] represents = s.split(" ");
//			 String[] represents = this.myParser(s);
			 
//			 String[] represents = array.toStrings();
//			 if(this.isNode(array)){

			 if(this.isNode(s)){
//				 node = array.toStrings();
				 node = s;
				 represents = this.myParser(node);
				 Long currentDistance = Long.parseLong(represents[1]);
				 if(currentDistance<dmin){
					 dmin = currentDistance;
				 }
			 }else{
				 Long d = Long.parseLong(s);
				 if(d<dmin){
			 		 dmin = new Long(d);
				 }
			 }
		 }
			 represents[1]=dmin.toString();
			 String newNode="";
			 for(int i=0; i<represents.length;i++){
				newNode=newNode+" "+represents[i]; 
			 }
			 newNode=newNode.trim();
//			 node[1]= dmin.toString(); //change the node's distance to dmin
//			 ArrayWritable nodeArray = new ArrayWritable(node);
//			 context.write(nodeId, nodeArray);
			 context.write(nodeId, new Text(newNode));

		 
		 
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
	
	private boolean isNode(String s){
		if(s.contains(" ")){
			return true;
		}
		return false;
	}
    String[] myParser(String s){
    	String[] array = new String[3];
    	String entry = "";
    	int index = 0;
    	for(int i = 0; i<s.length(); i++){
    		if(s.charAt(i)==' '){
    			if(index<3){
    				array[index]=entry;
    				entry="";
    				index++;
    			}
    		}else{
    			entry = entry + s.charAt(i);
    		}
    	}
		if(entry.length()>0 && index<4){
			array[index]=entry;
		}
    	return array;
    }
}
