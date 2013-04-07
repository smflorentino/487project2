package graphs;

import java.util.List;

public class Node {
	private Long id;
	private Long distance;
	private List<Long> neighbors;
	
	public Node(Long id, Long distance, List<Long> neighbors){
		this.id = id;
		this.distance = distance;
		this.neighbors = neighbors;
	}
	
	public Long getId(){
		return this.id;
	}
	
	public Long getDistance(){
		return this.distance;
	}
	
	public void setDistance(long distance){
		this.distance = distance;
	}
	
	public List<Long> getNeighbors(){
		return this.neighbors;
	}
	
	@Override
	public String toString(){
		return this.id.toString() + "\t" + this.distance.toString() + "\t" + this.neighbors.toString();
	}

}
