package streaming;

public class Edge {
	//int id;
	int belong;
	int belong0;
	Vertex[] pointVertex;
	boolean in=false;
	boolean inq=false;
	boolean ing=false;
	public Edge(/*int id, */int belong) {
		// TODO Auto-generated constructor stub
		//this.id=id;
		this.belong=belong;
		this.belong0=belong;
		pointVertex=new Vertex[2];
	}
	
	
}
