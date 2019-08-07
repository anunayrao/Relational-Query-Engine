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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Orderbymem {
	public static ArrayList<String> orderby(ArrayList<String> listLast,ArrayList<ColumnDefinition> groupbycd,List<OrderByElement> order ) throws IOException {
		  String blocks="inmemfile";
		  //int blocksize =2;
		 ArrayList<String> ColumnNames =new ArrayList<>();
			ArrayList<ColDataType> datatypes =new ArrayList();
			ArrayList<ColumnDefinition> columns =groupbycd;
			ArrayList<Integer> indexes = new ArrayList<>();
			ArrayList<MyTable> tables = new ArrayList();
			HashMap<Integer,MyTable> tableinfo =  new HashMap();
		
			for (int i=0;i<columns.size();i++) {
				 String colName = columns.get(i).getColumnName().toLowerCase();
				 ColumnNames.add(colName);
		         ColDataType colDataType = columns.get(i).getColDataType();
		         datatypes.add(colDataType);
		            
		           
			} 
			//System.out.println(order+"ORDER!!");
			//System.out.println(ColumnNames+"ColNames!!!");
			for (OrderByElement o : order) {
				
				int k = 0;
			for(String s : ColumnNames) {
					if(o.toString().toLowerCase().contains(s.toLowerCase())) {
						indexes.add(k);
					}
					else if(ConfigureVariables.testselect.get(k).toString().toLowerCase().contains(o.toString().toLowerCase()))
					{
						indexes.add(k);
					}
					k++;
				}
				
			}
			//System.out.println(indexes);
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
		
		Collections.sort(listLast,comparison);
		return listLast;
	}
	}



	 class Compare implements Comparator<String>{
			
		 public int increase( int i)
		 {
		 	i++;
		 	return i;
		 	
		 }
		 int i=0 ;
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
			if((datatypes.get(indexes.get(i)).toString().equalsIgnoreCase("String"))||(datatypes.get(indexes.get(i)).toString().contains("CHAR"))||(datatypes.get(indexes.get(i)).toString().equalsIgnoreCase("varchar"))) {
				if(ConfigureVariables.desc) {
					if(first[indexes.get(i)].compareTo(second[indexes.get(i)])<0)
					{
					returnval = 1;
					}
				if(first[indexes.get(i)].compareTo(second[indexes.get(i)])>0)
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
				else {
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
			}
			
			if(datatypes.get(indexes.get(i)).toString().equalsIgnoreCase( "int" ) )
			{
				if(!ConfigureVariables.desc) {
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
}
	 






//class SortComparator implements Comparator<>() 
