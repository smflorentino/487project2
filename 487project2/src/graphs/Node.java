package graphs;

import java.util.List;

public class Node {
	private Long id;
	private Long distance;
	private List<Long> neighbors;
	private boolean isVisited;
	
	public Node(Long id, Long distance, List<Long> neighbors, boolean isVisited){
		this.id = id;
		this.distance = distance;
		this.neighbors = neighbors;
		this.isVisited = isVisited;
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
	
	public boolean getIsVisited(){
		return this.isVisited;
	}
	
	public void setIsVisited(boolean isVisited){
		this.isVisited = isVisited;
	}
	
	@Override
	public String toString(){
		return this.id.toString() + "\t" + this.distance.toString() + "\t" + this.neighbors.toString() +"\t" + isVisited;
	}

}
