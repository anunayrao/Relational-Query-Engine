package dubstep;
////@Author - Apoorva Biseria
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

//import net.sf.js
//import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
//import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.OrderByElement;
public class ExternalSort {
	public  void orderbyGroup(MyTable t, List<Object> GroupBy) throws IOException {
	 int blocks=1;
	  int blocksize =10000;
	 ArrayList<String> ColumnNames = t.columnNames;
		ArrayList<ColDataType> datatypes =t.columnDatatype;
		ArrayList<ColumnDefinition> columns = t.tableColDef;
		ArrayList<Integer> indexes = new ArrayList<>();
		ArrayList<MyTable> tables = new ArrayList();
		HashMap<Integer,MyTable> tableinfo =  new HashMap();
	
		/*for (int i=0;i<columns.size();i++) {
			 String colName = columns.get(i).getColumnName().toLowerCase();
			 ColumnNames.add(colName);
	         ColDataType colDataType = columns.get(i).getColDataType();
	         datatypes.add(colDataType);
	            
	           
		}*/
		
for (Object o : GroupBy) {
			
			int k = 0;
		for(String s : ColumnNames) {
				if(o.toString().toLowerCase().contains(s.toLowerCase())) {
					indexes.add(k);
				}
				
				k++;
			
			}}

		
			
	//System.out.println("indexes"+indexes);	
		
		
		/*
		for(String s:ColumnNames)
		System.out.println(s);
		for(ColDataType s:datatypes)
			System.out.println(s);
		for(Integer s:indexes)
			System.out.println(s);
			*/
		
		Compare comparison = new Compare(ColumnNames,datatypes,indexes);
	//CREATE TABLE Q(A STRING,B INT);
		//SELECT Q.A FROM Q ORDER BY B,A;
	FileReader fr =new FileReader(ConfigureVariables.temppath+t.tableName+ConfigureVariables.FILEEXTENSION);
	BufferedReader br = new BufferedReader(fr);
	ArrayList<String> list = new ArrayList<String>();
	String row;
	while((row=t.readtuple())!=null) {
		
		if(list.size()<blocksize)
		list.add(row);
		if(list.size() ==blocksize) {
			Collections.sort(list,comparison);
			FileWriter fileWriter =new FileWriter(ConfigureVariables.temppath+blocks+ConfigureVariables.FILEEXTENSION);
			BufferedWriter  bw = new BufferedWriter(fileWriter);
			for(String s :list)
				bw.write(s+"\n");
			bw.close();
			String name =Integer.toString(blocks);
			ArrayList<ColumnDefinition> ck = t.tableColDef;
			MyTable table = new MyTable(name,ck,false);
			tableinfo.put(blocks, table);
			list.clear();
			blocks++;
		}
		
		
		
		
	}
	if(list.size()<blocksize &&list.size()>0 && row==null) {
		Collections.sort(list,comparison);
		FileWriter fileWriter =new FileWriter(ConfigureVariables.temppath+blocks+ConfigureVariables.FILEEXTENSION);
		BufferedWriter  bw = new BufferedWriter(fileWriter);
		for(String s :list)
			bw.write(s+"\n");
		bw.close();
		String name =Integer.toString(blocks);
		ArrayList<ColumnDefinition> ck = t.tableColDef;
		MyTable table = new MyTable(name,ck,false);
		tableinfo.put(blocks, table);
		list.clear();
		blocks++;
	}
	for(int i:tableinfo.keySet())
	{
		tables.add(tableinfo.get(i));
	}
	mergeFiles(blocks,ColumnNames, datatypes, indexes,tableinfo ,tables);
	
	br.close();
	}
	

	public  void orderby(MyTable t,List<OrderByElement> order ) throws IOException {
		  int blocks=1;
		  int blocksize =10000;
		 ArrayList<String> ColumnNames =new ArrayList<>();
			ArrayList<ColDataType> datatypes =new ArrayList();
			ArrayList<ColumnDefinition> columns = t.tableColDef;
			ArrayList<Integer> indexes = new ArrayList<>();
			ArrayList<MyTable> tables = new ArrayList();
			HashMap<Integer,MyTable> tableinfo =  new HashMap();
		
			for (int i=0;i<columns.size();i++) {
				 String colName = columns.get(i).getColumnName().toLowerCase();
				 ColumnNames.add(colName);
		         ColDataType colDataType = columns.get(i).getColDataType();
		         datatypes.add(colDataType);
		            
		           
			}
			
for (OrderByElement o : order) {
				
				int k = 0;
			for(String s : ColumnNames) {
					if(o.toString().toLowerCase().contains(s.toLowerCase())) {
						indexes.add(k);
					}
					
					k++;
				
				}}
			
		//System.out.println("indexes"+indexes);	
			for(OrderByElement o:order) {
				if(o.toString().contains("DESC"))
				{
					ConfigureVariables.desc =true;
				}
			}
			
			/*
			for(String s:ColumnNames)
			System.out.println(s);
			for(ColDataType s:datatypes)
				System.out.println(s);
			for(Integer s:indexes)
				System.out.println(s);  */
			
			Compare comparison = new Compare(ColumnNames,datatypes,indexes);
		//CREATE TABLE Q(A STRING,B INT);
			//SELECT Q.A FROM Q ORDER BY B,A;
		FileReader fr =new FileReader(ConfigureVariables.temppath+t.tableName+ConfigureVariables.FILEEXTENSION);
		BufferedReader br = new BufferedReader(fr);
		ArrayList<String> list = new ArrayList<String>();
		String row;
		while((row=t.readtuple())!=null) {
			
			if(list.size()<blocksize)
			list.add(row);
			if(list.size() ==blocksize) {
				Collections.sort(list,comparison);
				FileWriter fileWriter =new FileWriter(ConfigureVariables.temppath+blocks+ConfigureVariables.FILEEXTENSION);
				BufferedWriter  bw = new BufferedWriter(fileWriter);
				for(String s :list)
					bw.write(s+"\n");
				bw.close();
				String name =Integer.toString(blocks);
				ArrayList<ColumnDefinition> ck = t.tableColDef;
				MyTable table = new MyTable(name,ck,false);
				tableinfo.put(blocks, table);
				list.clear();
				blocks++;
			}
			
			
			
			
		}
		if(list.size()<blocksize &&list.size()>0 && row==null) {
			Collections.sort(list,comparison);
			FileWriter fileWriter =new FileWriter(ConfigureVariables.temppath+blocks+ConfigureVariables.FILEEXTENSION);
			BufferedWriter  bw = new BufferedWriter(fileWriter);
			for(String s :list)
				bw.write(s+"\n");
			bw.close();
			String name =Integer.toString(blocks);
			ArrayList<ColumnDefinition> ck = t.tableColDef;
			MyTable table = new MyTable(name,ck,false);
			tableinfo.put(blocks, table);
			list.clear();
			blocks++;
		}
		for(int i:tableinfo.keySet())
		{
			tables.add(tableinfo.get(i));
		}
		mergeFiles(blocks,ColumnNames, datatypes, indexes,tableinfo ,tables);
		
		br.close();
		}
	public  void mergeFiles(int blocks,ArrayList<String> ColumnNames,ArrayList<ColDataType> datatypes,ArrayList<Integer> indexes,HashMap<Integer,MyTable >tableinfo,ArrayList<MyTable>tables) throws IOException {
		Compare comparison = new Compare( ColumnNames,datatypes, indexes);
		PriorityQueue<String> priorityqueue = new PriorityQueue<String>(blocks,comparison);
		FileWriter fileWriter =new FileWriter(ConfigureVariables.temppath+"finalfile"+ConfigureVariables.FILEEXTENSION);
		BufferedWriter  bw = new BufferedWriter(fileWriter);
		for(int  i = 0;i<tables.size();i++)
		{	
		priorityqueue.add(tables.get(i).readtuple()+"|"+i);
		}
		while(priorityqueue.size()!=0) {
			String data;
			int k= priorityqueue.peek().lastIndexOf("|");
			String temp = priorityqueue.peek().substring(k+1,priorityqueue.peek().length());
			String row1=  priorityqueue.peek().substring(0,k);
					
			//String row[] = row1.split("^");
			//System.out.println(row1);
			//System.out.println(row.length);
			int blocknumber = Integer.parseInt(temp);
			bw.write(row1+"\n");
			priorityqueue.poll();
			if((data= tables.get(blocknumber).readtuple())!=null)
			priorityqueue.add(data+"|"+blocknumber);
			
		}
		bw.close();
		
	}
	//CREATE TABLE Q(A STRING,B INT,C DATE);
	//SELECT Q.A FROM Q ORDER BY B,C;
	public int increase( int i)
	{
		i++;
		return i;
		
	}

		 class Compare implements Comparator<String>{
				int i ;
			 ArrayList<String> ColumnNames;
			 ArrayList<ColDataType> datatypes;
			 ArrayList<Integer> indexes;
			 
			public Compare(ArrayList<String> ColumnNames,ArrayList<ColDataType> datatypes,ArrayList<Integer> indexes) {
				this.ColumnNames = ColumnNames;
				this.datatypes =  datatypes;
				this.indexes = indexes;
				
				
				
			}
			int returnval;
			

			@Override
			public int compare(String s1, String s2) {
				//System.out.println(s1);
				//int i =0;
				
					
				
				String first [];
				String second [];
				
					 first = s1.split("\\|");
				
				
				 second =  s2.split("\\|");
				 
				//for(String s:first)
				//	System.out.println(s);
				//for(String s:second)
				//	System.out.println(s);
				//System.out.println(indexes.get(i));
				//System.out.println(datatypes.get(indexes.get(i)).toString());
				//System.out.println(Integer.parseInt(first[indexes.get(i)]));
				//System.out.println(Integer.parseInt(second[indexes.get(i)]));
				//if(Integer.parseInt(first[indexes.get(i)])>Integer.parseInt(second[indexes.get(i)]))
				//{	System.out.println("done");}
				if(datatypes.get(indexes.get(i)).toString().equalsIgnoreCase("String")) {
					if(first[indexes.get(i)].compareTo(second[indexes.get(i)])>0)
						{
						returnval = 1;
						}
					if(first[indexes.get(i)].compareTo(second[indexes.get(i)])<0)
					{
						returnval =  -1;}
					if(first[indexes.get(i)].compareTo(second[indexes.get(i)])==0)
					{	if(i<indexes.size()-1)
							{i=increase(i);
							//System.out.println(i+"ii");
						compare(s1,s2);}
					else {// i =0;
					returnval =  0;
					
					}
					}
				}
				
				if(datatypes.get(indexes.get(i)).toString().equalsIgnoreCase( "int" ) )
				{if(!ConfigureVariables.desc) {
					if(Integer.parseInt(first[indexes.get(i)])>Integer.parseInt(second[indexes.get(i)]))
					{
						returnval =  1;
						}
					else if(Integer.parseInt(first[indexes.get(i)])<Integer.parseInt(second[indexes.get(i)]))
						
						{
						returnval =  -1;}
					
					else {
						if(i<indexes.size()-1)
						{i=increase(i);
						//System.out.println(i+"ii");
					compare(s1,s2);}
				else {// i =0;
				returnval =  0;
				
				}
						
						}
				}
				else {
					if(Integer.parseInt(first[indexes.get(i)])<Integer.parseInt(second[indexes.get(i)]))
						returnval=  1;
					else if(Integer.parseInt(first[indexes.get(i)])>Integer.parseInt(second[indexes.get(i)]))
						returnval = -1;
					
					else {
						if(i<indexes.size()-1)
						{i=increase(i);
						//System.out.println(i+"ii");
					compare(s1,s2);}
				else {// i =0;
				returnval =  0;
				
				}
						}
						
						
						}
					
				
				}	
				if(datatypes.get(indexes.get(i)).toString().equalsIgnoreCase( "decimal" ))
				{if(!ConfigureVariables.desc) {
					if(Double.parseDouble(first[indexes.get(i)])>Double.parseDouble(second[indexes.get(i)]))
					{
						returnval =  1;
						}
					else if(Double.parseDouble(first[indexes.get(i)])<Double.parseDouble(second[indexes.get(i)]))
						
						{
						returnval =  -1;}
					
					else {
						if(i<indexes.size()-1)
						{i=increase(i);
						//System.out.println(i+"ii");
					compare(s1,s2);}
				else {// i =0;
				returnval =  0;
				
				}
						
						}
				}
				else {
					if(Double.parseDouble(first[indexes.get(i)])<Double.parseDouble(second[indexes.get(i)]))
						returnval=  1;
					else if(Double.parseDouble(first[indexes.get(i)])>Double.parseDouble(second[indexes.get(i)]))
						returnval = -1;
					
					else {
						if(i<indexes.size()-1)
						{i=increase(i);
						//System.out.println(i+"ii");
					compare(s1,s2);}
				else {// i =0;
				returnval =  0;
				
				}
						}
						
						
						}
					
				}
					
			if(datatypes.get(indexes.get(i)).toString().equalsIgnoreCase( "DATE" )) 
						
					{
				try{SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		        Date date1 = sdf.parse(first[indexes.get(i)]);
		        Date date2= sdf.parse(second[indexes.get(i)]);
				
		        //System.out.println("date1 : " + sdf.format(date1));
		       // System.out.println("date2 : " + sdf.format(date2));
		        i = 0;
		        if (date1.compareTo(date2) > 0) {
		           
		        	returnval=  1;
		        } else if (date1.compareTo(date2) < 0) {
		           
		        	returnval = -1;
		        } else if (date1.compareTo(date2) == 0) {
		           
		        	returnval = 0;
		        } 

					}
			catch(ParseException e) {
				//System.out.println(e.getMessage());
			}
					}
				
							
				
				// TODO Auto-generated method stub
				 i =0 ;
				return returnval;
				
				
				
			}
}}

	 
	 
 


//class SortComparator implements Comparator<>() 
