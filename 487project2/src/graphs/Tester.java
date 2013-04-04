package graphs;

import java.util.List;


public class Tester {
	
	public static void main(String args[]){
	//Test String Array node representation -> Node object creation
	GraphsMapper m = new GraphsMapper();
	String[] s = {"1","1000000","2:3","Y"};
	Node node = m.getNode(s);
	System.out.println(node);
	
	//TODO: Test Input File (w/ tab delimiter) -> Array of Strings Parser

	
	}

}
