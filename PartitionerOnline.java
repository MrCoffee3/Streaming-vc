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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

public class PartitionerOnline {
	public List<Partition> partitionList;
	public List<Integer> pidList;
	public List<Vertex> vertexList;
	public int pNum;
	public int vertexNum;
	public int edgeNum;
	public float balance;
	public Random random;
	public int maxSize;
	public int minSize;
	public String data;
	public String initEdgePath;
	public String dynamicEdgePath;
	public int realMigrateEdges;
	public float rf;
	public float rff;
	public int total = 0;
	public int gnum = 0;//找到的总组团边数
	public int notmove = 0;//没有再移动的新增的边数量
	public int righttarget=0;//目标分区为新边所在分区的组团边数量
	public int realmovegnum=0;//移动了的组团边数量
	public int repgroupnum=0;//被复制的点所在的分区的组团边数量
	public int repright=0;//有组团边转移到新边所在分区的次数
	public int totalneedre=0;//需要找组团便的次数
	public String init = "";

	public PartitionerOnline(int pNum, float balance, int vNum, String iEP, String dEP, String data, String init)
			throws IOException {
		// TODO Auto-generated constructor stub
		this.rf = 0;
		this.rff = 0;
		this.pNum = pNum;
		this.balance = balance;
		this.partitionList = new ArrayList<Partition>();
		this.pidList = new ArrayList<Integer>();
		for (int i = 0; i < pNum; i++) {
			this.partitionList.add(new Partition(i));
			this.pidList.add(i);
		}
		this.vertexList = new ArrayList<Vertex>();
		this.edgeNum = 0;
		this.vertexNum = vNum;
		this.random = new Random();
		this.maxSize = 0;
		this.minSize = Integer.MAX_VALUE;
		this.data = data;
		this.initEdgePath = iEP;
		this.dynamicEdgePath = dEP;
		this.realMigrateEdges = 0;
		this.init = init;
	}

	public void loadInit() throws IOException {
		for (int i = 0; i < vertexNum; i++) {
			Vertex v = new Vertex(i + 1);
			v.belongs = new int[pNum];
			v.notingroup = new int[pNum];
			for (int j = 0; j < pNum; j++) {
				v.neighborEdges.add(new ArrayList<Edge>());
			}
			vertexList.add(v);
		}
		// System.out.println(vertexNum);
		BufferedReader in1;
		for (int i = 0; i < pNum; i++) {
			String fileName = "partition" + i + ".txt";
			in1 = new BufferedReader(new FileReader(new File(initEdgePath + "\\" + fileName)));// 初始边
			String string;
			while ((string = in1.readLine()) != null) {
				edgeNum++;
				String[] vertexs = string.split(" ");
				Vertex v1 = vertexList.get(Integer.parseInt(vertexs[0]) - 1);
				Vertex v2 = vertexList.get(Integer.parseInt(vertexs[1]) - 1);
				Edge edge = new Edge(i);
				edge.pointVertex[0] = v1;
				edge.pointVertex[1] = v2;
				if (v1.belongs[i] == 0) {
					v1.belongs[i] = 1;
					v1.belongnums++;
				}
				v1.neighborEdges.get(i).add(edge);
				if (v2.belongs[i] == 0) {
					v2.belongs[i] = 1;
					v2.belongnums++;
				}
				v2.neighborEdges.get(i).add(edge);
				partitionList.get(i).w++;
			}
			in1.close();
		}
		for (Partition partition : partitionList) {
			if (partition.w > maxSize) {
				maxSize = partition.w;
			}
			if (partition.w < minSize) {
				minSize = partition.w;
			}
		}
		float avg = (float) edgeNum / (float) pNum;
		int v = 0;
		for (Vertex vertex : vertexList) {
			int bnums = 0;
			for (int i = 0; i < pNum; i++) {
				if (vertex.belongs[i] == 1) {
					bnums++;
				}
			}
			if (bnums != 0) {
				v++;
			}
		}
		System.out.println("VertexNum:" + v + ";EdgeNum:" + edgeNum + ";Rep Factor:" + calRep() + ";maxbal:"
				+ ((maxSize - avg) / avg));
		/*
		 * for (Vertex vertex : vertexList) { System.out.print(vertex.id+": "); for (int
		 * i : vertex.belong) { System.out.print(i+" "); } System.out.println(); }
		 */
	}

	
	public void tracker() throws IOException {
		long t = 0;
		System.out.println("Dynamic and Starting Tracking!!");
		BufferedReader in2 = new BufferedReader(new FileReader(new File(dynamicEdgePath)));
		String string;
		int min = Integer.MAX_VALUE;
		int minId = -1;
		int newedges = 0;
		while ((string = in2.readLine()) != null) {
			edgeNum++;
			newedges++;
			String[] vertexs = string.split(" ");
			Vertex v1 = vertexList.get(Integer.parseInt(vertexs[0]) - 1);
			Vertex v2 = vertexList.get(Integer.parseInt(vertexs[1]) - 1);
			int belong = -1;
			Vertex rep = null;
			Edge edge = new Edge(-1);
			edge.pointVertex[0] = v1;
			edge.pointVertex[1] = v2;
			long t1 = System.currentTimeMillis();
			
			
			
			
			
			if (v1.belongnums != 0 && v2.belongnums != 0) {
				Object[] result = calBelong(edge);
				belong = (int) result[0];
				rep = (Vertex) result[1];
			} else if (v1.belongnums == 0 && v2.belongnums == 0) {
				min = Integer.MAX_VALUE;
				minId = -1;
				for (Partition partition : partitionList) {
					if (partition.w < min) {
						min = partition.w;
						minId = partition.id;
					}
				}
				belong = minId;
			} else {
				min = Integer.MAX_VALUE;
				minId = -1;
				if (v1.belongnums == 0) {
					for (int i = 0; i < pNum; i++) {
						if (v2.belongs[i] == 1) {
							Partition partition = partitionList.get(i);
							if (partition.w < min) {
								min = partition.w;
								minId = i;
							}
						}
					}
				} else {
					for (int i = 0; i < pNum; i++) {
						if (v1.belongs[i] == 1) {
							Partition partition = partitionList.get(i);
							if (partition.w < min) {
								min = partition.w;
								minId = i;
							}
						}
					}
				}
				belong = minId;
			}
			edge.belong = belong;
			edge.belong0 = belong;
			if (v1.belongs[belong] == 0) {
				v1.belongs[belong] = 1;
				v1.belongnums++;
			}
			if (v2.belongs[belong] == 0) {
				v2.belongs[belong] = 1;
				v2.belongnums++;
			}
			v1.neighborEdges.get(belong).add(edge);
			v2.neighborEdges.get(belong).add(edge);
			partitionList.get(belong).w++;

			// System.out.println((String) result[2] + ";" + reps.size());
			long t2 = System.currentTimeMillis();
			t += t2 - t1;
			updateMaxMin(partitionList.get(belong));
			
			if (rep != null) {
				Queue<Group> gQueue = new PriorityQueue<Group>(new groupCompare());
				int[][] map12 = new int[pNum][pNum];// v1的邻居所在分区的分布
				int[][] map21 = new int[pNum][pNum];// v2的邻居所在分区的分布
				for (List<Edge> list : v1.neighborEdges) {
					for (Edge edge2 : list) {
						Vertex vertex = edge2.pointVertex[0].id == v1.id ? edge2.pointVertex[1] : edge2.pointVertex[0];
						for (int i = 0; i < pNum; i++) {
							if (vertex.belongs[i] == 1 && i != edge2.belong) {
								map12[edge2.belong][i]++;
							}
						}
					}
				}
				for (List<Edge> list : v2.neighborEdges) {
					for (Edge edge2 : list) {
						Vertex vertex = edge2.pointVertex[0].id == v2.id ? edge2.pointVertex[1] : edge2.pointVertex[0];
						for (int i = 0; i < pNum; i++) {
							if (vertex.belongs[i] == 1 && i != edge2.belong) {
								map21[edge2.belong][i]++;
							}
						}
					}
				}
				long maxtg = 0;
				for (int i = 0; i < pNum; i++) {

					if (i != belong && rep.belongs[i] == 1) {
						// v1所在的第i个分区
						int[] ovc = map12[i];
						int maxovc = 0;
						int maxp = i;
						for (int i1 = 0; i1 < pNum; i1++) {
							if (ovc[i1] > maxovc) {
								maxovc = ovc[i1];
								maxp = i1;
							} else if (ovc[i1] == maxovc) {
								if (partitionList.get(i1).w < partitionList.get(i).w) {
									maxp = i1;
								}
							}
						}
						int ivc = 0;
						Set<Vertex> v1neighbor = new HashSet<Vertex>();
						v1neighbor.add(rep);
						for (Edge edge2 : rep.neighborEdges.get(i)) {
							if (edge2.ing == false) {

								Vertex vertex = edge2.pointVertex[0].id == rep.id ? edge2.pointVertex[1]
										: edge2.pointVertex[0];
								if (vertex.belongs[maxp] == 1) {
									v1neighbor.add(vertex);
								}
							}
						}
						long t3 = System.currentTimeMillis();
						for (Vertex vertex : v1neighbor) {
							for (Edge edge3 : vertex.neighborEdges.get(i)) {
								Vertex vertex2 = edge3.pointVertex[0].id == vertex.id ? edge3.pointVertex[1]
										: edge3.pointVertex[0];
								if (!v1neighbor.contains(vertex2)) {
									ivc++;
									break;
								}
							}
						}
						long t4 = System.currentTimeMillis();
						if (maxovc - ivc >= 1) {
							Group group = new Group();
							group.belong = i;
							group.target = maxp;
							group.maxovc = maxovc;
							group.ovc = ovc;
							group.ivc = ivc;
							group.vertexGroup.addAll(v1neighbor);

							for (Vertex vertex : v1neighbor) {
								for (Edge edge2 : vertex.neighborEdges.get(i)) {
									Vertex vertex1 = edge2.pointVertex[0].id == vertex.id ? edge2.pointVertex[1]
											: edge2.pointVertex[0];
									if (edge2.ing == false) {
										if (v1neighbor.contains(vertex1)) {

											group.edgeGroup.add(edge2);
											edge2.ing = true;
										} else {
											vertex.notingroup[i] = 1;
										}
									}
								}
							}
							gQueue.add(group);
							gnum++;
						}
						if (t4 - t3 > maxtg) {
							maxtg = t4 - t3;
						}
					}
				}
				Set<Integer> targeted = new HashSet<Integer>();
				long t5 = System.currentTimeMillis();
				if(!gQueue.isEmpty()) {
					totalneedre++;
				}
				boolean isright=false;
				while (!gQueue.isEmpty()) {
					Group group = gQueue.poll();
					for (Edge edge2 : group.edgeGroup) {
						edge2.ing = false;
					}
					if (targeted.contains(group.belong)) {
						for (Vertex vertex : group.vertexGroup) {
							vertex.notingroup[group.belong] = 0;
						}
						continue;
					}
					realmovegnum++;
					repgroupnum++;
					if(group.target==belong) {
						isright=true;
						righttarget++;
					}
					realMigrateEdges += group.edgeGroup.size();
					targeted.add(group.target);
					for (int i = 0; i < group.vertexGroup.size(); i++) {
						Vertex vertex2 = group.vertexGroup.get(i);
						if (vertex2.notingroup[group.belong] == 1 && vertex2.belongs[group.target] == 1) {
							// ivc&&ovc
							vertex2.notingroup[group.belong] = 0;
						} else if (vertex2.notingroup[group.belong] == 1 && vertex2.belongs[group.target] == 0) {// ivc
							vertex2.belongs[group.target] = 1;
							vertex2.belongnums++;
							vertex2.notingroup[group.belong] = 0;
						} else if (vertex2.notingroup[group.belong] == 0 && vertex2.belongs[group.target] == 1) {// ovc
							vertex2.belongs[group.belong] = 0;
							vertex2.belongnums--;
							vertex2.notingroup[group.belong] = 0;
						} else {
							vertex2.belongs[group.belong] = 0;
							vertex2.belongs[group.target] = 1;
							vertex2.notingroup[group.belong] = 0;
						}
					}
					for (int i = 0; i < group.edgeGroup.size(); i++) {
						Edge edge1 = group.edgeGroup.get(i);
						edge1.belong = group.target;
						for (Vertex vertex2 : edge1.pointVertex) {
							vertex2.neighborEdges.get(group.belong).remove(edge1);
							vertex2.neighborEdges.get(group.target).add(edge1);
						}
						edge1.inq = false;
						partitionList.get(group.belong).w--;
						partitionList.get(group.target).w++;
					}
				}
				if(isright) {
					repright++;
				}
				long t6 = System.currentTimeMillis();
				t += t6 - t5;
				for (int i = 0; i < pNum; i++) {
					Vertex another = rep.id == v1.id ? v2 : v1;
					if (another.belongs[i] == 1) { // v2所在的第i个分区
						int[] ovc = map12[i];
						int maxovc = 0;
						int maxp = i;
						for (int i1 = 0; i1 < pNum; i1++) {
							if (ovc[i1] > maxovc) {
								maxovc = ovc[i1];
								maxp = i1;
							} else if (ovc[i1] == maxovc) {
								if (partitionList.get(i1).w < partitionList.get(i).w) {
									maxp = i1;
								}
							}
						}
						int ivc = 0;
						Set<Vertex> v1neighbor = new HashSet<Vertex>();
						v1neighbor.add(another);
						for (Edge edge2 : another.neighborEdges.get(i)) {
							if (edge2.ing == false) {

								Vertex vertex = edge2.pointVertex[0].id == another.id ? edge2.pointVertex[1]
										: edge2.pointVertex[0];
								if (vertex.belongs[maxp] == 1) {
									v1neighbor.add(vertex);
								}
							}
						}
						long t3 = System.currentTimeMillis();
						for (Vertex vertex : v1neighbor) {
							for (Edge edge3 : vertex.neighborEdges.get(i)) {
								Vertex vertex2 = edge3.pointVertex[0].id == vertex.id ? edge3.pointVertex[1]
										: edge3.pointVertex[0];
								if (!v1neighbor.contains(vertex2)) {
									ivc++;
									break;
								}
							}
						}
						long t4 = System.currentTimeMillis();
						if (maxovc - ivc >= 1) {
							Group group = new Group();
							group.belong = i;
							group.target = maxp;
							group.maxovc = maxovc;
							group.ovc = ovc;
							group.ivc = ivc;
							group.vertexGroup.addAll(v1neighbor);

							for (Vertex vertex : v1neighbor) {
								for (Edge edge2 : vertex.neighborEdges.get(i)) {
									Vertex vertex1 = edge2.pointVertex[0].id == vertex.id ? edge2.pointVertex[1]
											: edge2.pointVertex[0];
									if (edge2.ing == false) {
										if (v1neighbor.contains(vertex1)) {

											group.edgeGroup.add(edge2);
											edge2.ing = true;
										} else {
											vertex.notingroup[i] = 1;
										}
									}
								}
							}
							gQueue.add(group);
							gnum++;
						}
						if (t4 - t3 > maxtg) {
							maxtg = t4 - t3;
						}
					}
				}
				t += maxtg;
				targeted = new HashSet<Integer>();
				t5 = System.currentTimeMillis();
				while (!gQueue.isEmpty()) {
					Group group = gQueue.poll();
					for (Edge edge2 : group.edgeGroup) {
						edge2.ing = false;
					}
					if (targeted.contains(group.belong)) {
						for (Vertex vertex : group.vertexGroup) {
							vertex.notingroup[group.belong] = 0;
						}
						continue;
					}
					/*for (Vertex vertex : group.vertexGroup) {
						if(vertex.belongs[group.target]==1) {
							int neinum=0;
							int belongnum=0;
							for (List<Edge> list : vertex.neighborEdges) {
								
								neinum+=list.size();
							}
							for (int i : vertex.belongs) {
								if(i==1) {
									belongnum++;
								}
							}
							System.out.println(neinum+" "+belongnum);
						}
					}*/
					realMigrateEdges += group.edgeGroup.size();
					realmovegnum++;
					targeted.add(group.target);
					for (int i = 0; i < group.vertexGroup.size(); i++) {
						Vertex vertex2 = group.vertexGroup.get(i);
						if (vertex2.notingroup[group.belong] == 1 && vertex2.belongs[group.target] == 1) {
							// ivc&&ovc
							vertex2.notingroup[group.belong] = 0;
						} else if (vertex2.notingroup[group.belong] == 1 && vertex2.belongs[group.target] == 0) {// ivc
							vertex2.belongs[group.target] = 1;
							vertex2.belongnums++;
							vertex2.notingroup[group.belong] = 0;
						} else if (vertex2.notingroup[group.belong] == 0 && vertex2.belongs[group.target] == 1) {// ovc
							vertex2.belongs[group.belong] = 0;
							vertex2.belongnums--;
							vertex2.notingroup[group.belong] = 0;
						} else {
							vertex2.belongs[group.belong] = 0;
							vertex2.belongs[group.target] = 1;
							vertex2.notingroup[group.belong] = 0;
						}
					}
					for (int i = 0; i < group.edgeGroup.size(); i++) {
						Edge edge1 = group.edgeGroup.get(i);
						edge1.belong = group.target;
						for (Vertex vertex2 : edge1.pointVertex) {
							vertex2.neighborEdges.get(group.belong).remove(edge1);
							vertex2.neighborEdges.get(group.target).add(edge1);
						}
						edge1.inq = false;
						partitionList.get(group.belong).w--;
						partitionList.get(group.target).w++;
					}
				}
				t6 = System.currentTimeMillis();
				t += t6 - t5;
			}
			if (edge.belong == edge.belong0) {
				notmove++;
			}
		}
		in2.close();
		float maxw = 0;
		for (Partition partition : partitionList) {
			if (partition.w > maxw) {
				maxw = partition.w;
			}
		}
		float avg = (float) edgeNum / (float) pNum;
		System.out.println("After Dynamic:");
		System.out.println("VertexNum:" + vertexNum + ";EdgeNum:" + edgeNum + ";Rep Factor:" + calRep() + ";maxbal:"
				+ ((maxw - avg) / avg));
		System.out.println("time for tracking:" + t);
		System.out.println("find group:"+gnum);
		System.out.println("real move group:"+realmovegnum+";"+realmovegnum*1.0/gnum);
		System.out.println("need re--reright--pro:"+totalneedre+"--"+repright+"--"+repright*1.0/totalneedre);
		System.out.println("target is assignment:"+righttarget+";"+righttarget*1.0/repgroupnum);
		System.out.println("notmove:" + notmove + ";" + notmove * 1.0 / newedges);
	}

	public void HDRFTracker() throws IOException {
		long t = 0;
		System.out.println("Dynamic and Starting <<HDRF>> Tracking!!");
		BufferedReader in2 = new BufferedReader(new FileReader(new File(dynamicEdgePath)));
		String string;
		while ((string = in2.readLine()) != null) {
			edgeNum++;
			String[] vertexs = string.split(" ");
			Vertex v1 = vertexList.get(Integer.parseInt(vertexs[0]) - 1);
			Vertex v2 = vertexList.get(Integer.parseInt(vertexs[1]) - 1);
			int belong = -1;
			Edge edge = new Edge(-1);
			edge.pointVertex[0] = v1;
			edge.pointVertex[1] = v2;
			long t1 = System.currentTimeMillis();
			belong = calBelongForHDRF(edge);
			edge.belong = belong;
			edge.belong0 = belong;
			if (v1.belongs[belong] == 0) {
				v1.belongs[belong] = 1;
				v1.belongnums++;
			}
			if (v2.belongs[belong] == 0) {
				v2.belongs[belong] = 1;
				v2.belongnums++;
			}
			v1.neighborEdges.get(belong).add(edge);
			v2.neighborEdges.get(belong).add(edge);
			Partition partition = partitionList.get(belong);
			// partition.edges.add(edge);
			partition.w++;
			long t2 = System.currentTimeMillis();
			t += t2 - t1;
			updateMaxMin(partition);
		}
		in2.close();
		float maxw = 0;
		for (Partition partition : partitionList) {
			if (partition.w > maxw) {
				maxw = partition.w;
			}
		}
		float avg = (float) edgeNum / (float) pNum;
		System.out.println("After Dynamic:");
		System.out.println("VertexNum:" + vertexNum + ";EdgeNum:" + edgeNum + ";Rep Factor:" + calRep() + ";maxbal:"
				+ ((maxw - avg) / avg));
		System.out.println("time for HDRF tracking:" + t);
	}

	public void randomTracker() throws IOException {
		long t = 0;
		Random random = new Random();
		System.out.println("Dynamic and Starting <<Random>> Tracking!!");
		BufferedReader in2 = new BufferedReader(new FileReader(new File(dynamicEdgePath)));
		String string;
		while ((string = in2.readLine()) != null) {
			edgeNum++;
			String[] vertexs = string.split(" ");
			Vertex v1 = vertexList.get(Integer.parseInt(vertexs[0]) - 1);
			Vertex v2 = vertexList.get(Integer.parseInt(vertexs[1]) - 1);
			long t1 = System.currentTimeMillis();
			int belong = random.nextInt(pNum);
			Edge edge = new Edge(belong);
			edge.pointVertex[0] = v1;
			edge.pointVertex[1] = v2;
			if (v1.belongs[belong] == 0) {
				v1.belongs[belong] = 1;
				v1.belongnums++;
			}
			if (v2.belongs[belong] == 0) {
				v2.belongs[belong] = 1;
				v2.belongnums++;
			}
			v1.neighborEdges.get(belong).add(edge);
			v2.neighborEdges.get(belong).add(edge);
			Partition partition = partitionList.get(belong);
			// partition.edges.add(edge);
			partition.w++;
			long t2 = System.currentTimeMillis();
			t += t2 - t1;
			updateMaxMin(partition);
		}
		in2.close();
		float maxw = 0;
		for (Partition partition : partitionList) {
			if (partition.w > maxw) {
				maxw = partition.w;
			}
		}
		float avg = (float) edgeNum / (float) pNum;
		System.out.println("After Dynamic:");
		System.out.println("VertexNum:" + vertexNum + ";EdgeNum:" + edgeNum + ";Rep Factor:" + calRep() + ";maxbal:"
				+ ((maxw - avg) / avg));
		System.out.println("time for random tracking:" + t);
	}

	public void greedyTracker() throws IOException {
		long t = 0;
		System.out.println("Dynamic and Starting <<greedy>> Tracking!!");
		BufferedReader in2 = new BufferedReader(new FileReader(new File(dynamicEdgePath)));
		String string;
		while ((string = in2.readLine()) != null) {
			edgeNum++;
			String[] vertexs = string.split(" ");
			Vertex v1 = vertexList.get(Integer.parseInt(vertexs[0]) - 1);
			Vertex v2 = vertexList.get(Integer.parseInt(vertexs[1]) - 1);
			int belong = -1;
			Edge edge = new Edge(belong);
			edge.pointVertex[0] = v1;
			edge.pointVertex[1] = v2;
			long t1 = System.currentTimeMillis();
			belong = calBelongForGreedy(edge);
			edge.belong = belong;
			edge.belong0 = belong;
			if (v1.belongs[belong] == 0) {
				v1.belongs[belong] = 1;
				v1.belongnums++;
			}
			if (v2.belongs[belong] == 0) {
				v2.belongs[belong] = 1;
				v2.belongnums++;
			}
			v1.neighborEdges.get(belong).add(edge);
			v2.neighborEdges.get(belong).add(edge);
			Partition partition = partitionList.get(belong);
			// partition.edges.add(edge);
			partition.w++;
			long t2 = System.currentTimeMillis();
			t += t2 - t1;
			updateMaxMin(partition);
		}
		in2.close();
		float maxw = 0;
		for (Partition partition : partitionList) {
			if (partition.w > maxw) {
				maxw = partition.w;
			}
		}
		float avg = (float) edgeNum / (float) pNum;
		System.out.println("After Dynamic:");
		System.out.println("VertexNum:" + vertexNum + ";EdgeNum:" + edgeNum + ";Rep Factor:" + calRep() + ";maxbal:"
				+ ((maxw - avg) / avg));
		System.out.println("time for greedy tracking:" + t);
	}

	public Object[] calBelong(Edge edge) {
		Object[] result = new Object[2];
		Vertex v1 = edge.pointVertex[0];
		Vertex v2 = edge.pointVertex[1];
		List<Integer> candidate1 = new ArrayList<Integer>();
		List<Integer> candidate2 = new ArrayList<Integer>();
		int[] belong = new int[pNum];
		for (int i = 0; i < pNum; i++) {
			if (v1.belongs[i] == 1) {
				belong[i]++;
			}
		}
		for (int i = 0; i < pNum; i++) {
			if (v2.belongs[i] == 1) {
				belong[i]++;
			}
		}
		for (int i = 0; i < pNum; i++) {
			if (belong[i] == 2) {
				candidate2.add(i);
				candidate1.clear();
			} else if (candidate2.size() == 0 && belong[i] == 1) {
				candidate1.add(i);
			}
		}
		if (candidate2.size() != 0) {
			int min = Integer.MAX_VALUE;
			int minId = -1;
			for (Integer i : candidate2) {
				Partition partition = partitionList.get(i);
				if (partition.w < min) {
					min = partition.w;
					minId = i;
				}
			}
			result[0] = minId;
			result[1] = null;
			return result;
		} else if (candidate1.size() != 0) {
			float lamda = 1f;
			float max = -100;
			int maxid = -1;
			int[][] map12 = new int[pNum][pNum];
			int[][] map21 = new int[pNum][pNum];
			for (List<Edge> list : v1.neighborEdges) {
				for (Edge edge2 : list) {
					Vertex vertex = edge2.pointVertex[0].id == v1.id ? edge2.pointVertex[1] : edge2.pointVertex[0];
					for (int i = 0; i < pNum; i++) {
						if (vertex.belongs[i] == 1 && i != edge2.belong) {
							map12[edge2.belong][i]++;
						}
					}
				}
			}
			for (List<Edge> list : v2.neighborEdges) {
				for (Edge edge2 : list) {
					Vertex vertex = edge2.pointVertex[0].id == v2.id ? edge2.pointVertex[1] : edge2.pointVertex[0];
					for (int i = 0; i < pNum; i++) {
						if (vertex.belongs[i] == 1 && i != edge2.belong) {
							map21[edge2.belong][i]++;
						}
					}
				}
			}
			for (int i = 0; i < pNum; i++) {
				if (v1.belongs[i] == 1) {
					int p21 = 0;
					int dg12 = 0;
					for (int j = 0; j < pNum; j++) {
						if (v2.belongs[j] == 1) {
							p21 += map21[j][i];
							dg12 += map12[i][j];
						}
					}
					float score = (float) (((p21) * 1.0)/* / ((dg12 + 1) * 1.0) */
							+ lamda * (maxSize - partitionList.get(i).w) * 1.0 / ((maxSize - minSize + 1) * 1.0));
					if (score > max) {
						max = score;
						maxid = i;
						result[1] = v2;
					} else if (score == max) {
						if (partitionList.get(i).w < partitionList.get(maxid).w) {
							maxid = i;
						}
					}
				}
			}
			for (int i = 0; i < pNum; i++) {
				if (v2.belongs[i] == 1) {

					int p12 = 0;
					int dg21 = 0;
					for (int j = 0; j < pNum; j++) {
						if (v1.belongs[j] == 1) {

							p12 += map12[j][i];
							dg21 += map21[i][j];
						}
					}
					float score = (float) (((p12) * 1.0)/* / ((dg21 + 1) * 1.0) */
							+ lamda * (maxSize - partitionList.get(i).w) * 1.0 / ((maxSize - minSize + 1) * 1.0));
					if (score > max) {
						max = score;
						maxid = i;
						result[1] = v1;
					} else if (score == max) {
						if (partitionList.get(i).w < partitionList.get(maxid).w) {
							maxid = i;
						}
					}
				}
			}
			result[0] = maxid;
			return result;
		} else {
			System.out.println("wrong!!");
			return result;
		}
	}

	public int calBelongForHDRF(Edge edge) {
		Vertex v1 = edge.pointVertex[0];
		Vertex v2 = edge.pointVertex[1];
		int c = 1;
		float[] belong = new float[pNum];
		float lamda = 1.1f;
		float max = 0;
		int maxid = 0;
		int dv1 = 0;
		int dv2 = 0;
		for (List<Edge> list : v1.neighborEdges) {
			dv1 += list.size();
		}
		for (List<Edge> list : v2.neighborEdges) {
			dv2 += list.size();
		}
		// System.out.println(dv1+";"+dv2);
		for (int i = 0; i < pNum; i++) {
			if (v1.belongs[i] == 1) {
				// System.out.println((dv1 * 1.0) / ((dv1 + dv2) * 1.0));
				belong[i] += (2 - (dv1 * 1.0) / ((dv1 + dv2) * 1.0));
				// belong[i] +=1;
			}
			if (v2.belongs[i] == 1) {
				// System.out.println((dv2 * 1.0) / ((dv1 + dv2) * 1.0));
				belong[i] += (2 - (dv2 * 1.0) / ((dv1 + dv2) * 1.0));
				// belong[i] +=1;
			}
			belong[i] += lamda * ((maxSize - partitionList.get(i).w) * 1.0 / ((c + maxSize - minSize) * 1.0));
			if (belong[i] > max) {
				max = belong[i];
				maxid = i;
			}
		}
		return maxid;
	}

	public int calBelongForGreedy(Edge edge) {
		Vertex v1 = edge.pointVertex[0];
		Vertex v2 = edge.pointVertex[1];
		float[] belong = new float[pNum];
		float max = 0;
		int maxid = 0;
		for (int i = 0; i < pNum; i++) {
			if (v1.belongs[i] == 1) {
				belong[i] += 1;
			}
			if (v2.belongs[i] == 1) {
				belong[i] += 1;
			}
			belong[i] += ((maxSize - partitionList.get(i).w) * 1.0 / ((1 + maxSize - minSize) * 1.0));
			if (belong[i] > max) {
				max = belong[i];
				maxid = i;
			}
		}
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
					break;
				}
			}
			minSize = min;
		}
	}

	public float calRep() {
		int total = 0;
		for (Vertex vertex : vertexList) {
			total += vertex.belongnums;
			/*int n=0;
			for (int i=0;i<pNum;i++) {
				if(vertex.neighborEdges.get(i).size()!=0) {
					n++;
				}
			}
			if(n!=vertex.belongnums) {
				System.out.println(n);
			}*/
		}
		int vnum = 0;
		for (Vertex vertex : vertexList) {
			if (vertex.belongnums != 0) {
				vnum++;
			}
		}
		System.out.println(total+";"+vnum);
		this.total = total;
		return (float) (total * 1.0 / vnum);
	}

	/*
	 * public float calRep2() { int total = 0; for (Vertex vertex : vertexList) {
	 * total += vertex.belongnums; }
	 * 
	 * return (float) (total * 1.0 / vertexNum); }
	 */

	public void output() throws IOException {
		/*BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("D:\\dataset\\result\\" + init + "\\" + data + "-" + pNum + ".txt")));*/
		int m = 0;
		//int[] pv=new int[pNum];
		for (Vertex vertex : vertexList) {
			/*for (int i=0;i<pNum;i++) {
				if(vertex.belongs[i]==1) {
					pv[i]++;
				}
			}*/
			for (List<Edge> edges : vertex.neighborEdges) {
				for (Edge edge : edges) {
					if (edge.in == false) {
						Vertex v1 = null;
						Vertex v2 = null;
						if (edge.pointVertex[0].id < edge.pointVertex[1].id) {
							v1 = edge.pointVertex[0];
							v2 = edge.pointVertex[1];
						} else {
							v1 = edge.pointVertex[1];
							v2 = edge.pointVertex[2];
						}
						//out.write(v1.id + " " + v2.id + " " + edge.belong + "\n");
						if (edge.belong != edge.belong0) {
							m++;
						}
						edge.in = true;
					}
				}
			}
		}
		//out.close();
		/*for (int i : pv) {
			System.out.print(i+" ");
		}
		System.out.println();*/
		float tmr = (float) (m * 1.0 / edgeNum);
		float rmr = (float) (realMigrateEdges * 1.0 / edgeNum);
		System.out.println("total migrate " + m + " edges " + tmr);
		System.out.println("real migrate " + realMigrateEdges + " edges " + rmr);
		System.out.println("avggroupsize:" + realMigrateEdges * 1.0 / realmovegnum);
	}

	public class myCompare implements Comparator<Integer> {

		@Override
		public int compare(Integer i1, Integer i2) {
			Partition p1 = partitionList.get(i1);
			Partition p2 = partitionList.get(i2);
			// TODO Auto-generated method stub
			if (p1.w > p2.w) {
				return -1;
			} else if (p1.w < p2.w) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	public class myCompare1 implements Comparator<Vertex> {

		@Override
		public int compare(Vertex v1, Vertex v2) {
			// TODO Auto-generated method stub
			if (v1.belongnums > v2.belongnums) {
				return -1;
			} else if (v1.belongnums < v2.belongnums) {
				return 1;
			} else {
				return 0;
			}
		}

	}

	public class groupCompare implements Comparator<Group> {

		@Override
		public int compare(Group g1, Group g2) {
			// TODO Auto-generated method stub
			if (g1.maxovc > g2.maxovc) {
				return -1;
			} else if (g1.maxovc < g2.maxovc) {
				return 1;
			} else {
				return 0;
			}
		}

	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String[] datas = { "dblp"/*, "roadNet-PA", "youtube", "lj", "sp", "orkut"*/ };
		int[] ps = { /*2, 4,*/ 8/*, 16*/ };
		String[] trackers = { "tracker", "hdrf", "greedy", "random" };
		for (String string : datas) {
			String data = string;
			for (int i : ps) {
				int pNum = i;
				for (String string2 : trackers) {
					String tracker = string2;
					// int pNum = 8;// 分区数2,4,8,16
					float balance = 1.1f;// 平衡系数
					// String data = "dblp";// 数据集
					String inputOrder = "RDM";// 输入顺序，随机或BFS
					int ipercent = 50;// 初始分区的百分比50,80
					// String tracker = "tracker";// tracker,hdrf,(greedy),random
					int vNum = 0;// 点数量317080,1088092,1134890,3997962,7600000,3072441
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
					String iEP;
					PartitionerOnline partitioner;
					String dEP = "D:\\dataset\\input_for_realtime\\" + data + "2-" + inputOrder + "-" + (100 - ipercent) + "%.txt";
					/*
					 * if (tracker.equals("hdrf")) { iEP =
					 * "D:\\dataset\\hdrf\\" + data + "2-" + inputOrder + "-partition-" + pNum; }
					 */
					System.out.println(
							"------------" + data + "-" + pNum + "-" + ipercent + "%-" + tracker + "------------");

					// partitioner.markDis();
					// long t1;
					// long t2;
					switch (tracker) {
					case "tracker":
						// t1 = System.currentTimeMillis();
						String[] inits = { "hdrf", "greedy", "random" };
						for (String init : inits) {
							System.out.println("init:" + init + "--------");
							iEP = "D:\\dataset\\Initial_partitioning\\" + init + "\\" + data + "2-" + inputOrder + "-partition-" + pNum + "-"
									+ ipercent + "%";
							partitioner = new PartitionerOnline(pNum, balance, vNum, iEP, dEP, data, init);
							partitioner.loadInit();
							partitioner.tracker();
							partitioner.output();
						}
						// t2 = System.currentTimeMillis();
						// System.out.println("time for tracking:" + (t2 - t1) * 1.0 / 1000);
						break;
					case "hdrf":
						iEP = "D:\\dataset\\Initial_partitioning\\hdrf\\" + data + "2-" + inputOrder + "-partition-" + pNum + "-" + ipercent
								+ "%";
						partitioner = new PartitionerOnline(pNum, balance, vNum, iEP, dEP, data, null);
						partitioner.loadInit();
						partitioner.HDRFTracker();
						break;
					case "random":
						// t1 = System.currentTimeMillis();
						iEP = "D:\\dataset\\Initial_partitioning\\random\\" + data + "2-" + inputOrder + "-partition-" + pNum + "-" + ipercent
								+ "%";
						partitioner = new PartitionerOnline(pNum, balance, vNum, iEP, dEP, data, null);
						partitioner.loadInit();
						partitioner.randomTracker();
						// t2 = System.currentTimeMillis();
						// System.out.println("time for random tracking:" + (t2 - t1) * 1.0 / 1000);
						break;
					case "greedy":
						// t1 = System.currentTimeMillis();
						iEP = "D:\\dataset\\Initial_partitioning\\greedy\\" + data + "2-" + inputOrder + "-partition-" + pNum + "-" + ipercent
								+ "%";
						partitioner = new PartitionerOnline(pNum, balance, vNum, iEP, dEP, data, null);
						partitioner.loadInit();
						partitioner.greedyTracker();
						// t2 = System.currentTimeMillis();
						// System.out.println("time for greedy tracking:" + (t2 - t1) * 1.0 / 1000);
					default:
						break;
					}
					// long t3 = System.currentTimeMillis();
					// partitioner.rePartition();
					// long t4 = System.currentTimeMillis();
					// System.out.println("time for repartition:" + (t4 - t3) * 1.0 / 1000);

				}
			}
		}
	}

}
