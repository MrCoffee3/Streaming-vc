package streaming;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Group {
	List<Edge> edgeGroup;
	List<Vertex> vertexGroup;
	Set<Vertex> overtices;
	Set<Vertex> ivertices;
	int[] ovc;
	int belong;
	int ivc;
	int maxovc;
	int target;
	public Group() {
		// TODO Auto-generated constructor stub
		this.edgeGroup=new ArrayList<Edge>();
		this.vertexGroup=new ArrayList<Vertex>();
		this.overtices=new HashSet<Vertex>();
		this.ivertices=new HashSet<Vertex>();
	}
}
