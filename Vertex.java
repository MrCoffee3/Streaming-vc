package streaming;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Vertex {
	int id;
	List<List<Edge>> neighborEdges;
	//Set<Vertex> pre;
	int[] belongs;
	int belongnums=0;
	int[] notingroup;
	public Vertex(int id) {
		// TODO Auto-generated constructor stub
		this.id=id;
		this.neighborEdges=new ArrayList<List<Edge>>();
		//this.pre=new HashSet<Vertex>();
		//this.neighborEdges=new ArrayList<Edge>();
		//this.belong=new HashSet<Integer>();
		//this.dis=100000;
		//this.groups=new HashSet<Group>();
	}
	
}
