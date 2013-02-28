import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import ucar.ma2.Array;
import ucar.nc2.NCdumpW;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;


public class Hncdump {
	boolean dumpHead;
	boolean dumpVariable;
	String variable;
	boolean dumpVariableData;
	boolean getTable;
	String tableName;
	String filePath;
	String realPath;
	String namenode;
	String serde;
	String inputformat;
	String outputformat;
	Hncdump() throws ParserConfigurationException, SAXException, IOException{
		readConfs();
	}
	void readConfs() throws ParserConfigurationException, SAXException, IOException{
		FileInputStream conf= new FileInputStream("conf/conf.xml");
		DocumentBuilder db=DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document document =db.parse(conf);
		NodeList nl=document.getElementsByTagName("property");
		HashMap<String,String>map=new HashMap<String,String>();
		String key=null;
		String value=null;
		for(int i=0;i<nl.getLength();++i){
			Node node=nl.item(i);
			if(node.getNodeType()==Node.ELEMENT_NODE){
				NodeList list=node.getChildNodes();
				for(int j=0;j<list.getLength();++j){
					Node n=list.item(j);
					//System.out.println(n.getNodeName());
					if(n.getNodeType()==Node.ELEMENT_NODE&&!n.getNodeName().equals("null")&&!n.getNodeName().equals("description")){
						//System.out.println(n.getTextContent());
						if(n.getNodeName().equals("name")){
							key=n.getTextContent();
							//System.out.println("key:"+key);
						}else if(n.getNodeName().equals("value")){
							value=n.getTextContent();
							//System.out.println("value:"+value);
							map.put(key.toString(), value.toString());
						}
					}
				}
			}
			
		}
		namenode=map.get("namenode");
		serde=map.get("serde.class");
		inputformat=map.get("inputformat.class");
		outputformat=map.get("outputformat.class");
		//System.out.println(map.size());
		conf.close();
	}
	void dumpHead() throws IOException{
		PrintWriter pw=new PrintWriter(System.out,true);
		NCdumpW.printHeader(realPath, pw);
	}
	void dumpVariable()throws IOException{
		NetcdfFile ncfile=null;
		try{
		ncfile=new NetcdfFile(realPath);
		boolean exists=false;
		for(Variable var:ncfile.getVariables()){
			if(variable.equals(var.getName())){
				System.out.println(var.toString());
				exists=true;
				break;
			}
		}
		if(!exists){
			System.out.println("variable does not exist!");
		}
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if(ncfile!=null)
			ncfile.close();
		}
	}
	void dumpVariableData()throws IOException{
		NetcdfFile ncfile=null;
		
		try{
		ncfile=new NetcdfFile(realPath);
		boolean exists=false;
		for(Variable var:ncfile.getVariables()){
			if(variable.equals(var.getName())){
				Array arr=var.read();
				System.out.println("IndexCount:"+arr.getSize());
				System.out.println(arr.toString());
				exists=true;
				break;
			}
		}
		if(!exists){
			System.out.println("variable does not exist!");
		}
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if(ncfile!=null)
			ncfile.close();
		}
	}
	void getTable()throws IOException{
		StringBuilder strBuilder=new StringBuilder();
	
		NetcdfFile ncfile=null;
		
		try{
		ncfile=new NetcdfFile(realPath);
		strBuilder.append("create table "+tableName+"(");
		List<Variable> vars=ncfile.getVariables();
		for(int i=0;i<vars.size();++i){
			Variable var=vars.get(i);
			if(i!=vars.size()-1){
				strBuilder.append(var.getName()+" "+var.getDataType()+",");
				//strBuilder.append(var.getName()+" "+"string"+",");
			}else{
				strBuilder.append(var.getName()+" "+var.getDataType()+")");
				//strBuilder.append(var.getName()+" "+"string"+")");
			}
		}
		strBuilder.append(" ROW FORMAT SERDE \'"+serde+"\' STORED AS INPUTFORMAT \'"+inputformat+"\' OUTPUTFORMAT \'"+outputformat+"\';");
		System.out.println(strBuilder.toString());
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if(ncfile!=null)
			ncfile.close();
		}
	}
	public void printUsage(){
		System.out.println("hncdump [-help] [-h] [-v name] [-vd name] [-t] file");
		System.out.println("[-help] \t The usage of hncdump");
		System.out.println("[-h] \t\t Header information only, no data ");
		System.out.println("[-v name] \t Variable definition of name in the header");
		System.out.println("[-vd name] \t Variable data of name in the file");
		System.out.println("[-t] \t\t Generating the the command of creating table in hive for the file ");
	}
	public void run(String args[]) throws IOException{
		for(int i=0;i<args.length;++i){
			if("-help".equals(args[i])){
				printUsage();
				System.exit(0);
			}
			else if("-h".equals(args[i])){
				dumpHead=true;
			}
			else if("-v".equals(args[i])){
				dumpVariable=true;
				variable=args[++i];
			}
			else if("-vd".equals(args[i])){
				dumpVariableData=true;
				variable=args[++i];
			}else if("-t".equals(args[i])){
				getTable=true;
				tableName=args[++i];
			}else{
				filePath=args[i];
			}
		}
		if(filePath==null||!filePath.startsWith("/")){
			System.out.println("filePath is not specified!");
			return ;
		}else {
			realPath=namenode+filePath;
			if (dumpHead) {
				dumpHead();
				return;
			} else if (dumpVariable) {
				dumpVariable();
				return;
			} else if (dumpVariableData) {
				dumpVariableData();
				return;
			} else if (getTable) {
				getTable();
				return;
			}
		}
	}
	public static void main(String args[]) throws IOException, ParserConfigurationException, SAXException{
		Hncdump hncdump=new Hncdump();
		hncdump.run(args);
		//test(hncdump);
	}
	public static void test(Hncdump hncdump) throws IOException{
		//String []args=new String("Hncdump  -h /user/hive/warehouse/ocean2/ocean_0001_01.nc.0000").split(" ");
		//String []args=new String("Hncdump -t ocean4 /user/hive/warehouse/ocean2/ocean_0001_01.nc.0000").split(" ");
		//String []args=new String("Hncdump -h /user/yifeng/ocean2/ocean_0001_01.nc.0000").split(" ");
		String []args=new String("Hncdump -h /user/yifeng/data1/nc_data/test_1_2.nc").split(" ");
		//String []args=new String("Hncdump -help").split(" ");
		hncdump.run(args);
		System.out.println("test");
	}
}
