package graphs;

import java.util.List;


public class Tester {
	
	public static void main(String args[]){
	//Test String Array node representation -> Node object creation
	GraphsMapper m = new GraphsMapper();
	String[] s = {"1","1000000","2:3","Y"};
	Node node = m.getNode(s);
//	System.out.println(node);
	
	String testString = "1 0 2:";
	String[] result = m.myParser(testString);
	for(int i=0; i<result.length;i++){
		System.out.println(result[i]);
	}

	
	}

}
