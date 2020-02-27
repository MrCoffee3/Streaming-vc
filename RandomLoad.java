package streaming;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RandomLoad {
	public Partition[] partitionList;
	public List<Vertex> vertexList;
	public int pNum;
	public int vertexNum;
	public int edgeNum;
	public float balance;
	public int[] degree;
	public Random random;
	public int maxSize;
	public int minSize;
	public String data;
	public String edgePath;
	public int ipercent;
	String inputOrder = "RDM";
	BufferedWriter out[];

	public RandomLoad(int pNum, float balance, int vNum, String edgePath, String data, int ipercent, String inputOrder)
			throws IOException {
		// TODO Auto-generated constructor stub
		this.pNum = pNum;
		this.balance = balance;
		this.partitionList = new Partition[pNum];
		for (int i = 0; i < pNum; i++) {
			this.partitionList[i] = new Partition(i);
		}
		this.vertexList = new ArrayList<Vertex>();
		this.edgeNum = 0;
		this.vertexNum = vNum;
		this.random = new Random();
		this.maxSize = 0;
		this.minSize = 0;
		this.data = data;
		this.edgePath = edgePath;
		this.inputOrder = inputOrder;
		this.ipercent = ipercent;
		this.out = new BufferedWriter[pNum];

		if (ipercent != 100) {
			File file = new File(
					"D:\\dataset\\random\\" + data + "2-" + inputOrder + "-partition-" + pNum + "-" + ipercent + "%");
			if (!file.exists()) {
				file.mkdir();
			}
		} else {
			File file = new File("D:\\dataset\\random\\" + data + "2-" + inputOrder + "-partition-" + pNum);
			if (!file.exists()) {
				file.mkdir();
			}
		}
		for (int i = 0; i < pNum; i++) {
			if (ipercent != 100) {

				out[i] = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream("D:\\dataset\\random\\" + data + "2-" + inputOrder
								+ "-partition-" + pNum + "-" + ipercent + "%" + "\\partition" + i + ".txt")));
			} else {

				out[i] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\dataset\\random\\" + data
						+ "2-" + inputOrder + "-partition-" + pNum + "\\partition" + i + ".txt")));
			}
		}
		load();
	}

	public void load() throws IOException {

		for (int i = 0; i < vertexNum; i++) {
			Vertex v = new Vertex(i + 1);
			v.belongs = new int[pNum];
			// v.dis=new int[pNum];
			vertexList.add(v);
		}
		System.out.println(vertexNum);
		degree = new int[vertexNum];
		Arrays.fill(degree, 0);

	}

	public void streaming() throws IOException {
		String str1;
		BufferedReader in1 = new BufferedReader(new FileReader(new File(edgePath)));// ��
		while ((str1 = in1.readLine()) != null) {
			edgeNum++;
			// System.out.println(str1);
			String[] vertexs = str1.split(" ");
			Vertex vs = vertexList.get(Integer.parseInt(vertexs[0]) - 1);
			Vertex vt = vertexList.get(Integer.parseInt(vertexs[1]) - 1);
			Edge edge = new Edge(-1);
			edge.pointVertex[0] = vs;
			edge.pointVertex[1] = vt;
			degree[vs.id - 1]++;
			degree[vt.id - 1]++;
			// System.err.println(vs.id+";"+degree[vs.id - 1]);
			// System.err.println(vt.id+";"+degree[vt.id - 1]);
			int belong0 = calBelong(edge);
			edge.belong = belong0;
			out[belong0].write(edge.pointVertex[0].id + " " + edge.pointVertex[1].id + "\n");
			// partitionList[belong0].edges.add(edge);
			partitionList[belong0].w++;
			updateMaxMin(partitionList[belong0]);
			vs.belongs[belong0] = 1;
			vt.belongs[belong0] = 1;
			/*
			 * for (int i : edge.pointVertex) { degree[vertex.id - 1]++; }
			 */
			// vs.neighborEdges.add(edge);
			// vt.neighborEdges.add(edge);
		}
		System.out.println(edgeNum);
		for (BufferedWriter bufferedWriter : out) {
			bufferedWriter.close();
		}
		in1.close();
	}

	public int calBelong(Edge edge) {
		int maxid=random.nextInt(pNum);
		return maxid;
	}

	public void updateMaxMin(Partition partition) {
		if (partition.w > maxSize) {
			maxSize = partition.w;
		}
		if (partition.w - 1 == minSize) {
			int min = partition.w;
			for (Partition partition2 : partitionList) {
				if (partition2.w < min) {
					min = partition2.w;
				}
			}
			minSize = min;
		}
	}

	public float calRep() {
		int total = 0;
		int one = 0;
		for (Vertex vertex : vertexList) {
			int bnums = 0;
			for (int i = 0; i < pNum; i++) {
				if (vertex.belongs[i] == 1) {
					bnums++;
				}
			}
			if (bnums == 1) {
				one++;
			}
			total += bnums;
		}
		System.out.println(one * 1.0 / vertexNum);
		return (float) (total * 1.0 / vertexNum);
	}

	public void output() throws IOException {

		System.out.println("vertexnum:" + vertexNum);
		System.out.println("edgenum:" + edgeNum);
		System.out.println("avg:" + (float) edgeNum / (float) pNum);
		System.out.println("min:" + ((float) edgeNum / (float) pNum) * (2 - balance));
		System.out.println("max:" + ((float) edgeNum / (float) pNum) * balance);
		System.out.println("avgRep:" + calRep());
		for (Partition partition : partitionList) {
			System.out.println(partition.id + ":" + partition.w);
		}
		/*
		 * BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new
		 * FileOutputStream("D:\\dataset\\hdrf\\dblp-16-belong.txt"))); for (Vertex
		 * vertex : vertexList) { for (int i : vertex.belong) { out.write(i+" "); }
		 * out.write("\n"); } out.close();
		 */
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String[] datas = { "dblp", "roadNet-PA", "youtube", "lj", "sp", "orkut" };
		float balance = 1.02f;// ƽ��ϵ��
		int vNum = 0;// ������317080,1088092,1134890,3997962,7600000,3072441
		String inputOrder = "RDM";// �����˳�������BFS
		int ipercent = 100;// ��ʼ����Ϊ�ٷ�֮��50,80,100
		int[] ps = {/* 2, 4, 8, 16,*/32 };
		for (String data : datas) {
			switch (data) {
			case "dblp":
				vNum = 317080;
				break;
			case "roadNet-PA":
				vNum = 1088092;
				break;
			case "youtube":
				vNum = 1134890;
				break;
			case "lj":
				vNum = 3997962;
				break;
			case "sp":
				vNum = 7600000;
				break;
			case "orkut":
				vNum = 3072441;
				break;
			default:
				break;
			}
			for (int pNum : ps) {
				String edgePath;
				if (ipercent != 100) {
					edgePath = "D:\\dataset\\random\\" + data + "2-" + inputOrder + "-" + ipercent + "%.txt";
				} else {
					edgePath = "D:\\dataset\\random\\" + data + "2-" + inputOrder + ".txt";
				}
				RandomLoad greedy = new RandomLoad(pNum, balance, vNum, edgePath, data, ipercent, inputOrder);
				long t1 = System.currentTimeMillis();
				greedy.streaming();
				long t2 = System.currentTimeMillis();
				greedy.output();
				System.out.println("time:" + (t2 - t1) * 1.0 / 1000);
				/*for (Partition partition : greedy.partitionList) {
					int id = partition.id;
					for (Edge edge : partition.edges) {
						out[id].write(edge.pointVertex[0] + " " + edge.pointVertex[1] + "\n");
					}
					out[id].close();
				}*/
			}
		}
		// boolean isEighty = true;//�Ƿ�80%

	}

}
