package graphs;

import java.util.List;

import org.apache.hadoop.io.LongWritable;


public class Tester {
	
	public static void main(String args[]){
	//Test String Array node representation -> Node object creation
	GraphsMapper m = new GraphsMapper();
//	String[] s = {"1","1000000","2:3","Y"};
//	Node node = m.getNode(s);
//	System.out.println(node);
	
	String testString = "32 125 23:24:25:31:40:41:46:";
	String[] represents = m.myParser(testString);
	for(int i=0; i<represents.length;i++){
		System.out.println(represents[i]);
	}
	LongWritable distance = new LongWritable(Long.parseLong(represents[1]));

	
	}

}
