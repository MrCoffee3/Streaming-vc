package streaming;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RandomOrder {
	public List<String> edgeList;
	public String vertexPath;
	public String data;
	//public List<String> vertexList;
	public RandomOrder(String data, String vertexPath) {
		// TODO Auto-generated constructor stub
		edgeList = new ArrayList<String>(117185083);
		this.vertexPath = vertexPath;
		this.data = data;
		//this.vertexList=new ArrayList<String>();
	}

	public void load() throws IOException {
		BufferedReader in1 = new BufferedReader(new FileReader(new File(vertexPath)));// µã
		String str1;
		int i = 0;
		while ((str1 = in1.readLine()) != null) {
			i++;
			if(str1.length()!=0) {
				String[] strings = str1.split(" ");
				for (String string : strings) {
					edgeList.add(i + " " + string);
				}
			}
		}
		in1.close();
	}

	/*public void load() throws IOException{
		BufferedReader in1 = new BufferedReader(new FileReader(new File(vertexPath)));// µã
		String str1;
		while ((str1 = in1.readLine()) != null) {
			if(str1.length()!=0) {
				vertexList.add(str1);
			}
		}
		in1.close();
	}*/
	
	public void output() throws IOException {
		Collections.shuffle(edgeList);
		BufferedWriter out1 = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream("D:\\dataset\\hdrf\\" + data + "2-RDM.txt")));
		for(int i=0;i<edgeList.size();i++) {
			String string=edgeList.get(i);
			out1.write(string + "\n");
		}
		/*for (Integer integer : list) {
			List<String> list2=vertexList.get(integer-1);
			if(list2.size()!=0) {
				Collections.shuffle(list2);
				for (String string2 : list2) {
					System.out.println(integer+" "+string2);
					//out1.write(integer+" "+string2 + "\n");
				}
			}
		}*/
		out1.close();
		System.out.println(edgeList.size());
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String data = "orkut";
		String vertexPath = "D:\\dataset\\" + data + "2-single.txt";
		RandomOrder rOrder = new RandomOrder(data, vertexPath);
		rOrder.load();
		rOrder.output();
	}

}
