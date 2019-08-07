package dubstep;
////@Author - Anunay Rao
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class OnePassHashJoin {

	public static MyTable computeJoin(MyTable t1, MyTable t2) throws IOException {
		String joinColumn = null;
		String jcl=null;
		String jcr=null;
		ArrayList<joinSpec> jclist = new  ArrayList<joinSpec>();
		//System.out.println(t1.tableName + t2.tableName);
		if(t1.tableName.contains("|")) {
			String tblname[] = t1.tableName.split("\\|");
			
			String tableName1 = tblname[tblname.length -1];
			
			// Can be modified to check for all the join conditions for each table;
			for(int i =0; i<tblname.length;i++) {
				joinColumn = getJoinColumn(tblname[i], t2.tableName);
				if(joinColumn!=null) {
					jcl = tblname[i]+"."+joinColumn;
					jcr = t2.tableName+"."+joinColumn;
					joinSpec js = new joinSpec();
					js.jcl = jcl;
					js.jcr=jcr;
					jclist.add(js);
					//break;		//To get the most recent join condition for left deep plan
				}
			}
			
			//joinColumn = getJoinColumn(tableName1, t2.tableName);
		}
		else {
			joinColumn = getJoinColumn(t1.tableName, t2.tableName);	
			jcl = t1.tableName+"."+joinColumn;
			jcr = t2.tableName+"."+joinColumn;
			joinSpec js = new joinSpec();
			js.jcl = jcl;
			js.jcr=jcr;
			jclist.add(js);
			/*
			for(int i=0; i<jclist.size();i++) {
				System.out.println(jclist.get(i).jcl + ":::"+ jclist.get(i).jcr);
			}
			*/
			
		}
		
		
		
		//System.out.println("JOIN:"+joinColumn);
		
		ArrayList<ColumnDefinition> jointablecd = new ArrayList();
		for(ColumnDefinition cd : t1.tableColDef) {
			jointablecd.add(cd);
		}
		for(ColumnDefinition cd : t2.tableColDef) {
			jointablecd.add(cd);
		}
		/*
		for(int i=0; i<jclist.size();i++) {
			System.out.println(jclist.get(i).jcl + ":::"+ jclist.get(i).jcr);
		}
		*/
		
		MyTable joinresult = new MyTable(t1.tableName+"|"+t2.tableName,jointablecd,false);
		
		 ArrayList<String> cn = new ArrayList<String>();
	     ArrayList<ColDataType> cd = new ArrayList<ColDataType>();
	     
	        for(int i=0;i<t1.columnNames.size();i++) {
	        	cn.add(t1.columnNames.get(i));
	        	cd.add(t1.columnDatatype.get(i));
	        }
	        for(int i=0;i<t2.columnNames.size();i++) {
	        	cn.add(t2.columnNames.get(i));
	        	cd.add(t2.columnDatatype.get(i));
	        }
	        
	        
	       // System.out.println(cn);
	       // System.out.println(cd);
	        joinresult.columnDatatype = cd;
	        joinresult.columnNames =cn;
	        
	        
		HashMap<String,ArrayList<String>> hashtable = new HashMap();
		
		
		if(joinColumn==null) {
			
			CrossProduct(t1, t2);
			String r;
			/*
			while((r=joinresult.readtuple())!=null) {
				System.out.println(r);
			}
			*/
			return joinresult;
			
			//System.out.println("No join Condition");
		}
		int l,r;
		ArrayList<Integer> lind = new ArrayList<Integer>();
		ArrayList<Integer> rind = new ArrayList<Integer>();
		//int l = getcolIndex(t1.tableColDef, joinColumn);
		//int r = getcolIndex(t2.tableColDef, joinColumn);
		for(int i=0; i<jclist.size();i++) {
			joinSpec js = jclist.get(i);
			jcl = js.jcl;
			jcr = js.jcr;
			l = getcolIndex(t1.columnNames, jcl);
			r = getcolIndex(t2.columnNames,jcr);
			lind.add(l);
			rind.add(r);
		}
		//System.out.println(lind);
		//System.out.println("LIND::"+lind);
		//l = getcolIndex(t1.columnNames, jcl);
		//r = getcolIndex(t2.columnNames,jcr);
		//System.out.println("Left"+l+"Right"+r);
		/*
		if(t1.tableName.contains("|")) {
		String tblname[] = t1.tableName.split("\\|");
		String tableName1 = tblname[tblname.length -1];
		 l = getcolIndex(t1.columnNames,tableName1+"."+joinColumn);
		}
		else {
		 l = getcolIndex(t1.columnNames, jcl);
		}
		 r = getcolIndex(t2.columnNames,jcr);
		*/
		
		
		//System.out.println(l+":"+r);
		
		//int hashobjectsize = 2;
		
		
		
		
		String row=null;
		if(ConfigureVariables.istpch3 && t1.tableName.contains("customer")) {
		
			HashMap<String, ArrayList<Long>> hm=null ;
			Expression e = ConfigureVariables.tableCondition.get("CUSTOMER").get(0);
			String str = ((BinaryExpression) e).getRightExpression().toString();
			str = str.substring(1, str.length()-1);
			int count =0;
			if(!ConfigureVariables.firstjoin) {
			
				try {
					hm = Main.readMap("CUSTOMER.ind");
				} catch (ClassNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				ArrayList<Long> set = hm.get(str);
				//System.out.println(str);
				Collections.sort(set);
				
				FileReader fr;
				BufferedReader br;
				
					fr = new FileReader(ConfigureVariables.pathname+t1.tableName+ConfigureVariables.FILEEXTENSION);
					 br = new BufferedReader(fr);
				
				int skip =0;
				//int count =0;
				//int r =0;
				for(Long j : set) {
					//System.out.println(i);
					//System.out.println(skip);
					br.skip(j-skip);
					String rowread = br.readLine();
					//System.out.println(rowread);
					if(rowread==null) {
						break;
					}
					String[] rowval = rowread.split("\\|");
					
					String key="";
					 for(int i =0;i<lind.size();i++) {
						 key += rowval[lind.get(i)];
						 key +="|";
					 }
					 if(hashtable.containsKey(key)) {
							hashtable.get(key).add(row);
							
						}
						else {
							ArrayList<String> temp = new ArrayList();
							temp.add(row);
							hashtable.put(key,temp);
							
						}
					
					long len = rowread.length() + 1;
					skip = (int) (len+j);
					
				
				}
				
			}
			else {
				int ind =0;
				int size = t1.tuples.size();
				while(ind!=size) {
					row = t1.tuples.get(ind);
					ind = ind+1;
					//System.out.println("While:"+t1.tableColDef);
					String []rowval = row.split("\\|");
					 if(checkSelection(rowval, t1.tableColDef,t1.tableName))
					
					{
						 String key="";
						 for(int i =0;i<lind.size();i++) {
							 key += rowval[lind.get(i)];
							 key +="|";
						 }
					
					//System.out.println("Yes");
				
						//System.out.println(row);
						if(hashtable.containsKey(key)) {
							hashtable.get(key).add(row);
							
						}
						else {
							ArrayList<String> temp = new ArrayList();
							temp.add(row);
							hashtable.put(key,temp);
							
						}
						
					
					
					
					
				}
				}
			}
			
		}
		else if(ConfigureVariables.istpch12 && t1.tableName.equalsIgnoreCase("lineitem")) {
			Date startdate,enddate;
			String sdate=null, edate=null;
			NavigableMap<Date, ArrayList<Long>> nm;
			NavigableMap<Date, ArrayList<Long>> hnm = null;;
			ArrayList<Expression> exp = ConfigureVariables.tableCondition.get("LINEITEM");
			ArrayList<Expression> newexp = new ArrayList<Expression>();
			ArrayList<Expression> fexp = new ArrayList<Expression>();
			for(Expression ex : exp ) {
				String sexp = ((BinaryExpression) ex).getLeftExpression().toString();
				//System.out.println(sexp);
				if(sexp.contains("LINEITEM.RECEIPTDATE")) {
					//System.out.println("yes");
					fexp.add(ex);
				}
				else{
					newexp.add(ex);
				}
			}
			ConfigureVariables.tableCondition.put("LINEITEM",newexp);
			Expression e= fexp.get(0);
			if(e instanceof GreaterThanEquals) {
			String sidate = ((BinaryExpression) e).getRightExpression().toString();
			 sdate = sidate.substring(6, 16);
			}
			else {
				String eidate = ((BinaryExpression) e).getRightExpression().toString();
				//System.out.println(eidate);
				edate = eidate.substring(6, 16);
			}
			//System.out.println(sdate);
			Expression e1 = fexp.get(1);
			if(!(e1 instanceof GreaterThanEquals)) {
				String eidate = ((BinaryExpression) e1).getRightExpression().toString();
				 edate = eidate.substring(6, 16);
				
				}
				else {
					String sidate = ((BinaryExpression) e1).getRightExpression().toString();
					 sdate = sidate.substring(6, 16);
				}
			
			//String eidate = ((BinaryExpression) e1).getRightExpression().toString();
			//String edate = eidate.substring(6, 16);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				startdate = sdf.parse(sdate);
				enddate = sdf.parse(edate);
				//System.out.println(startdate+"::::"+enddate);
				 nm = Main.readNMap("LINEITEMR.ind");
				// System.out.println("NM SIZE SET:"+nm.size());
				 hnm = nm.subMap(startdate, true, enddate, false);
				// System.out.println("SUBNM SIZE SET:"+hnm.size());
			} catch (ParseException e11) {
				// TODO Auto-generated catch block
				e11.printStackTrace();
			} catch (ClassNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			FileReader fr=null;
			BufferedReader br=null;
			try {
				fr = new FileReader(ConfigureVariables.pathname+t1.tableName+ConfigureVariables.FILEEXTENSION);
				 br = new BufferedReader(fr);
			
			
			ArrayList<Long> set = new ArrayList<Long>();
			for(Date d : hnm.keySet()) {
				ArrayList<Long> bytes = hnm.get(d);
				set.addAll(bytes);
			}
			Collections.sort(set);
			//System.out.println(t1.tableName+set.size());
			int skip =0;
			//int count =0;
			//int r =0;
			for(Long j : set) {
				//System.out.println(i);
				//System.out.println(skip);
				br.skip(j-skip);
				String rowread = br.readLine();
				//System.out.println(rowread);
				//System.out.println(rowread);
				if(rowread==null) {
					break;
				}
				String rowval[] = rowread.split("\\|");
				if(ConfigureVariables.tableCondition.containsKey(t1.tableName)) {
					if(checkSelection(rowval, t1.tableColDef, t1.tableName)) {
						//String[] rowval = rowread.split("\\|");
						
						String key="";
						 for(int i =0;i<lind.size();i++) {
							 key += rowval[lind.get(i)];
							 key +="|";
						 }
						 if(hashtable.containsKey(key)) {
								hashtable.get(key).add(rowread);
								
							}
							else {
								ArrayList<String> temp = new ArrayList();
								temp.add(rowread);
								hashtable.put(key,temp);
								
							}
					}}
				//SelectionOperator.Selection(tuple,ConfigureVariables.tableStrings.get(i).toLowerCase());
				long len = rowread.length() + 1;
				skip = (int) (len+j);
				
			
			}
			} catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
		}
				
		else{
		//System.out.println("400:yes");
		int count =0;
		if(!ConfigureVariables.firstjoin) {
			//System.out.println("FirstJoin:"+t1.tableName);
			ArrayList<String> deleterows = new ArrayList<String>();
			if(ConfigureVariables.mapdelete.containsKey(t1.tableName)) {
				deleterows = ConfigureVariables.mapdelete.get(t1.tableName);
			
		while((row=t1.readtuple())!=null) {
			if(!deleterows.contains(row)){
			//System.out.println("While:"+t1.tableColDef);
			
			String []rowval = row.split("\\|");
			 if(checkSelection(rowval, t1.tableColDef,t1.tableName))
			
			{	String key="";
				 for(int i =0;i<lind.size();i++) {
					 key += rowval[lind.get(i)];
					 key +="|";
				 }
			
			//System.out.println("Yes");
		
				//System.out.println(row);
				if(hashtable.containsKey(key)) {
					hashtable.get(key).add(row);
					
				}
				else {
					ArrayList<String> temp = new ArrayList();
					temp.add(row);
					hashtable.put(key,temp);
					
				}
				
			
			
			
			
		}
		}
		}
		}
			else {
				//System.out.println("yes:no delete join"+t1.tableName);
				while((row=t1.readtuple())!=null) {
					
					//System.out.println("While:"+t1.tableColDef);
					
					String []rowval = row.split("\\|");
					 if(checkSelection(rowval, t1.tableColDef,t1.tableName))
					
					{	String key="";
						 for(int i =0;i<lind.size();i++) {
							 key += rowval[lind.get(i)];
							 key +="|";
						 }
					
					//System.out.println("Yes");
				
						//System.out.println(row);
						if(hashtable.containsKey(key)) {
							hashtable.get(key).add(row);
							
						}
						else {
							ArrayList<String> temp = new ArrayList();
							temp.add(row);
							hashtable.put(key,temp);
							
						}
						
					
					
					
					
				}
				
				}
				//System.out.println("Hashtable size after t1::"+t1.tableName+hashtable.size());
			}
		// Put Newly inserted rows into hashtable for join
		if(ConfigureVariables.mapinsert.containsKey(t1.tableName)) {
			for(String s : ConfigureVariables.mapinsert.get(t1.tableName)) {
				row = s;
				String []rowval = row.split("\\|");
				 if(checkSelection(rowval, t1.tableColDef,t1.tableName))
				
				{	String key="";
					 for(int i =0;i<lind.size();i++) {
						 key += rowval[lind.get(i)];
						 key +="|";
					 }
				
				//System.out.println("Yes");
			
					//System.out.println(row);
					if(hashtable.containsKey(key)) {
						hashtable.get(key).add(row);
						
					}
					else {
						ArrayList<String> temp = new ArrayList();
						temp.add(row);
						hashtable.put(key,temp);
						
					}
					
				
				
				
				
			}
			}
		}
		
		}
		else {
			//System.out.println("Making map for"+t1.tableName);
			int ind =0;
			int size = t1.tuples.size();
			while(ind!=size) {
				row = t1.tuples.get(ind);
				ind = ind+1;
				//System.out.println("While:"+t1.tableColDef);
				String []rowval = row.split("\\|");
				 if(checkSelection(rowval, t1.tableColDef,t1.tableName))
				
				{
					 String key="";
					 for(int i =0;i<lind.size();i++) {
						 key += rowval[lind.get(i)];
						 key +="|";
					 }
				
				//System.out.println("Yes");
			
					//System.out.println(row);
					if(hashtable.containsKey(key)) {
						hashtable.get(key).add(row);
						
					}
					else {
						ArrayList<String> temp = new ArrayList();
						temp.add(row);
						hashtable.put(key,temp);
						
					}
					
				
				
				
				
			}
			}
		}
	}
		
		
		if(ConfigureVariables.istpch5&& t2.tableName.equals("ORDERS")) {
			Date startdate,enddate;
			String sdate=null, edate=null;
			NavigableMap<Date, ArrayList<Long>> nm;
			NavigableMap<Date, ArrayList<Long>> hnm = null;;
			Expression e = ConfigureVariables.tableCondition.get("ORDERS").get(0);
			if(e instanceof GreaterThanEquals) {
			String sidate = ((BinaryExpression) e).getRightExpression().toString();
			 sdate = sidate.substring(6, 16);
			}
			else {
				String eidate = ((BinaryExpression) e).getRightExpression().toString();
				edate = eidate.substring(6, 16);
			}
			//System.out.println(sdate);
			Expression e1 = ConfigureVariables.tableCondition.get("ORDERS").get(1);
			if(!(e1 instanceof GreaterThanEquals)) {
				String eidate = ((BinaryExpression) e1).getRightExpression().toString();
				 edate = eidate.substring(6, 16);
				
				}
				else {
					String sidate = ((BinaryExpression) e1).getRightExpression().toString();
					 sdate = sidate.substring(6, 16);
				}
			
			//String eidate = ((BinaryExpression) e1).getRightExpression().toString();
			//String edate = eidate.substring(6, 16);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				startdate = sdf.parse(sdate);
				enddate = sdf.parse(edate);
				//System.out.println(startdate+"::::"+enddate);
				 nm = Main.readNMap("ORDERS.ind");
				 //System.out.println("NM SIZE SET:"+nm.size());
				 hnm = nm.subMap(startdate, true, enddate, false);
				// System.out.println("SUBNM SIZE SET:"+hnm.size());
			} catch (ParseException e11) {
				// TODO Auto-generated catch block
				e11.printStackTrace();
			} catch (ClassNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
			FileReader fr;
			BufferedReader br;
			try {
				fr = new FileReader(ConfigureVariables.pathname+t2.tableName+ConfigureVariables.FILEEXTENSION);
				 br = new BufferedReader(fr);
			
			
			ArrayList<Long> set = new ArrayList<Long>();
			for(Date d : hnm.keySet()) {
				ArrayList<Long> bytes = hnm.get(d);
				set.addAll(bytes);
			}
			Collections.sort(set);
			
			int skip =0;
			//int count =0;
			//int r =0;
			for(Long i : set) {
				//System.out.println(i);
				//System.out.println(skip);
				br.skip(i-skip);
				String rowread = br.readLine();
				//System.out.println(rowread);
				if(rowread==null) {
					break;
				}
				String[] rowval = rowread.split("\\|");
				
				String key="";
				 for(int k =0;k<rind.size();k++) {
					 key += rowval[rind.get(k)];
					 key +="|";
				 }
				if(hashtable.containsKey(key)) {
					 ArrayList<String> value = hashtable.get(key);
        			for(String s: value) {
        				//System.out.println(s+"|"+row);
        				joinresult.tuples.add(s+"|"+rowread);
        				//writeresult(s+"|"+row, joinresult);
        			}
				}
				long len = rowread.length() + 1;
				skip = (int) (len+i);
				
			
			}
			} catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
		//System.out.println(t2.tableName+":"+ConfigureVariables.countcheck);	
		} 
		else if(ConfigureVariables.istpch3 && t2.tableName.equalsIgnoreCase("orders")) {
			
			Date startdate;
			NavigableMap<Date, ArrayList<Long>> nm;
			NavigableMap<Date, ArrayList<Long>> hnm = null;;
			Expression e = ConfigureVariables.tableCondition.get("ORDERS").get(0);
			String sidate = ((BinaryExpression) e).getRightExpression().toString();
			String sdate = sidate.substring(6, 16);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				startdate = sdf.parse(sdate);
				//System.out.println(startdate+"::::"+enddate);
				 nm = Main.readNMap("ORDERS.ind");
				 //System.out.println("NM SIZE SET:"+nm.size());
				 hnm = nm.headMap(startdate, false);
				// System.out.println("SUBNM SIZE SET:"+hnm.size());
			} catch (ParseException e11) {
				// TODO Auto-generated catch block
				e11.printStackTrace();
			} catch (ClassNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			FileReader fr;
			BufferedReader br;
			try {
				fr = new FileReader(ConfigureVariables.pathname+t2.tableName+ConfigureVariables.FILEEXTENSION);
				 br = new BufferedReader(fr);
			
			
			ArrayList<Long> set = new ArrayList<Long>();
			for(Date d : hnm.keySet()) {
				ArrayList<Long> bytes = hnm.get(d);
				set.addAll(bytes);
			}
			Collections.sort(set);
			
			int skip =0;
			//int count =0;
			//int r =0;
			for(Long i : set) {
				//System.out.println(i);
				//System.out.println(skip);
				br.skip(i-skip);
				String rowread = br.readLine();
				//System.out.println(rowread);
				if(rowread==null) {
					break;
				}
				String[] rowval = rowread.split("\\|");
				
				String key="";
				 for(int k =0;k<rind.size();k++) {
					 key += rowval[rind.get(k)];
					 key +="|";
				 }
				if(hashtable.containsKey(key)) {
					 ArrayList<String> value = hashtable.get(key);
        			for(String s: value) {
        				//System.out.println(s+"|"+row);
        				joinresult.tuples.add(s+"|"+rowread);
        				//writeresult(s+"|"+row, joinresult);
        			}
				}
				long len = rowread.length() + 1;
				skip = (int) (len+i);
				
			
			}
			} catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
		}
		else if(ConfigureVariables.istpch3 && t2.tableName.equalsIgnoreCase("lineitem")) {
			Date startdate;
			NavigableMap<Date, ArrayList<Long>> nm;
			NavigableMap<Date, ArrayList<Long>> hnm = null;;
			Expression e = ConfigureVariables.tableCondition.get("LINEITEM").get(0);
			String sidate = ((BinaryExpression) e).getRightExpression().toString();
			String sdate = sidate.substring(6, 16);
			//System.out.println(sdate);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				startdate = sdf.parse(sdate);
				//System.out.println(startdate+"::::"+enddate);
				 nm = Main.readNMap("LINEITEM.ind");
				 //System.out.println("NM SIZE SET:"+nm.size());
				 hnm = nm.tailMap(startdate, false);
				// System.out.println("SUBNM SIZE SET:"+hnm.size());
			} catch (ParseException e11) {
				// TODO Auto-generated catch block
				e11.printStackTrace();
			} catch (ClassNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			FileReader fr;
			BufferedReader br;
			try {
				fr = new FileReader(ConfigureVariables.pathname+t2.tableName+ConfigureVariables.FILEEXTENSION);
				 br = new BufferedReader(fr);
			
			
			ArrayList<Long> set = new ArrayList<Long>();
			for(Date d : hnm.keySet()) {
				ArrayList<Long> bytes = hnm.get(d);
				set.addAll(bytes);
			}
			Collections.sort(set);
			
			int skip =0;
			//int count =0;
			//int r =0;
			for(Long i : set) {
				//System.out.println(i);
				//System.out.println(skip);
				br.skip(i-skip);
				String rowread = br.readLine();
				//System.out.println(rowread);
				if(rowread==null) {
					break;
				}
				String[] rowval = rowread.split("\\|");
				
				String key="";
				 for(int k =0;k<rind.size();k++) {
					 key += rowval[rind.get(k)];
					 key +="|";
				 }
				if(hashtable.containsKey(key)) {
					 ArrayList<String> value = hashtable.get(key);
        			for(String s: value) {
        				//System.out.println(s+"|"+row);
        				joinresult.tuples.add(s+"|"+rowread);
        				//writeresult(s+"|"+row, joinresult);
        			}
				}
				long len = rowread.length() + 1;
				skip = (int) (len+i);
				
			
			}
			} catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
		}
		else if(ConfigureVariables.istpch12 && t2.tableName.equalsIgnoreCase("lineitem")) {
			Date startdate,enddate;
			String sdate=null, edate=null;
			NavigableMap<Date, ArrayList<Long>> nm;
			NavigableMap<Date, ArrayList<Long>> hnm = null;;
			ArrayList<Expression> exp = ConfigureVariables.tableCondition.get("LINEITEM");
			ArrayList<Expression> fexp = new ArrayList<Expression>();
			for(Expression ex : exp ) {
				String sexp = ((BinaryExpression) ex).getLeftExpression().toString();
				//System.out.println(sexp);
				if(sexp.contains("LINEITEM.RECEIPTDATE")) {
					//System.out.println("yes");
					fexp.add(ex);
				}
			}
			Expression e= fexp.get(0);
			if(e instanceof GreaterThanEquals) {
			String sidate = ((BinaryExpression) e).getRightExpression().toString();
			 sdate = sidate.substring(6, 16);
			}
			else {
				String eidate = ((BinaryExpression) e).getRightExpression().toString();
				//System.out.println(eidate);
				edate = eidate.substring(6, 16);
			}
			//System.out.println(sdate);
			Expression e1 = fexp.get(1);
			if(!(e1 instanceof GreaterThanEquals)) {
				String eidate = ((BinaryExpression) e1).getRightExpression().toString();
				 edate = eidate.substring(6, 16);
				
				}
				else {
					String sidate = ((BinaryExpression) e1).getRightExpression().toString();
					 sdate = sidate.substring(6, 16);
				}
			
			//String eidate = ((BinaryExpression) e1).getRightExpression().toString();
			//String edate = eidate.substring(6, 16);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				startdate = sdf.parse(sdate);
				enddate = sdf.parse(edate);
				//System.out.println(startdate+"::::"+enddate);
				 nm = Main.readNMap("LINEITEMR.ind");
				 //System.out.println("NM SIZE SET:"+nm.size());
				 hnm = nm.subMap(startdate, true, enddate, false);
				// System.out.println("SUBNM SIZE SET:"+hnm.size());
			} catch (ParseException e11) {
				// TODO Auto-generated catch block
				e11.printStackTrace();
			} catch (ClassNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			FileReader fr=null;
			BufferedReader br=null;
			try {
				fr = new FileReader(ConfigureVariables.pathname+t2.tableName+ConfigureVariables.FILEEXTENSION);
				 br = new BufferedReader(fr);
			
			
			ArrayList<Long> set = new ArrayList<Long>();
			for(Date d : hnm.keySet()) {
				ArrayList<Long> bytes = hnm.get(d);
				set.addAll(bytes);
			}
			Collections.sort(set);
			int skip =0;
			//int count =0;
			//int r =0;
			for(Long j : set) {
				//System.out.println(i);
				//System.out.println(skip);
				br.skip(j-skip);
				String rowread = br.readLine();
				//System.out.println(rowread);
				if(rowread==null) {
					break;
				}
				String rowval[] = rowread.split("\\|");
				if(ConfigureVariables.tableCondition.containsKey(t2.tableName)) {
					if(checkSelection(rowval, t2.tableColDef, t2.tableName)) {
						/*if(t2.tableName.equalsIgnoreCase("orders")){
							ConfigureVariables.countcheck = ConfigureVariables.countcheck + 1;
						}*/
						//System.out.println("yes2");
						String key="";
						 for(int i =0;i<rind.size();i++) {
							 key += rowval[rind.get(i)];
							 key +="|";
						 }
						if(hashtable.containsKey(key)) {
							 ArrayList<String> value = hashtable.get(key);
		         			for(String s: value) {
		         				//System.out.println(s+"|"+row);
		         				joinresult.tuples.add(s+"|"+row);
		         				//writeresult(s+"|"+row, joinresult);
		         			}
						}
					}}
				//SelectionOperator.Selection(tuple,ConfigureVariables.tableStrings.get(i).toLowerCase());
				long len = rowread.length() + 1;
				skip = (int) (len+j);
				
			
			}
			} catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
		}
		
		else {
		/*if(t2.tableName.equalsIgnoreCase("nation") ||t2.tableName.equalsIgnoreCase("customer")||t2.tableName.equalsIgnoreCase("lineitem")||t2.tableName.equalsIgnoreCase("supplier") ) {
			System.out.println(t2.tableName+":"+"traditional");
		}*/
			//System.out.println("hello");
			
		//System.out.println("t2 starting.."+t2.tableName);
		if(ConfigureVariables.mapdelete.containsKey(t2.tableName)) {
			
			while((row=t2.readtuple())!=null) {
				//System.out.println(row);
				if(!ConfigureVariables.mapdelete.get(t2.tableName).contains(row)){
				String[] rowval = row.split("\\|");
				/*if(t2.tableName.equalsIgnoreCase("ORDERS")) {
					ConfigureVariables.countcheck = ConfigureVariables.countcheck +1;
				}*/
				if(ConfigureVariables.tableCondition.containsKey(t2.tableName)) {
					
				if(checkSelection(rowval, t2.tableColDef, t2.tableName)) {
					/*if(t2.tableName.equalsIgnoreCase("orders")){
						ConfigureVariables.countcheck = ConfigureVariables.countcheck + 1;
					}*/
					//System.out.println("yes2");
					
					String key="";
					 for(int i =0;i<rind.size();i++) {
						 key += rowval[rind.get(i)];
						 key +="|";
					 }
					
					if(hashtable.containsKey(key)) {
						 ArrayList<String> value = hashtable.get(key);
	         			for(String s: value) {
	         				//System.out.println(s+"|"+row);
	         				joinresult.tuples.add(s+"|"+row);
	         				//writeresult(s+"|"+row, joinresult);
	         			}
					}
				}}
				else {
					//System.out.println("Hello");
					
					String key="";
					 for(int i =0;i<rind.size();i++) {
						 key += rowval[rind.get(i)];
						 key +="|";
					 }
					if(hashtable.containsKey(key)) {
						
						 ArrayList<String> value = hashtable.get(key);
	        			for(String s: value) {
	        				//System.out.println(s+"|"+row);
	        				joinresult.tuples.add(s+"|"+row);
	        				//writeresult(s+"|"+row, joinresult);
	        			}
					}
				}
			}
			}
		}
		else {
			//System.out.println("t2:no delete or insert"+t2.tableName);
		while((row=t2.readtuple())!=null) {
			//System.out.println(row);
			
			String[] rowval = row.split("\\|");
			/*if(t2.tableName.equalsIgnoreCase("ORDERS")) {
				ConfigureVariables.countcheck = ConfigureVariables.countcheck +1;
			}*/
			if(ConfigureVariables.tableCondition.containsKey(t2.tableName)) {
				
			if(checkSelection(rowval, t2.tableColDef, t2.tableName)) {
				/*if(t2.tableName.equalsIgnoreCase("orders")){
					ConfigureVariables.countcheck = ConfigureVariables.countcheck + 1;
				}*/
				//System.out.println("yes2");
				
				String key="";
				 for(int i =0;i<rind.size();i++) {
					 key += rowval[rind.get(i)];
					 key +="|";
				 }
				
				if(hashtable.containsKey(key)) {
					 ArrayList<String> value = hashtable.get(key);
         			for(String s: value) {
         				//System.out.println(s+"|"+row);
         				joinresult.tuples.add(s+"|"+row);
         				//writeresult(s+"|"+row, joinresult);
         			}
				}
			}}
			else {
				//System.out.println("Hello");
				
				String key="";
				 for(int i =0;i<rind.size();i++) {
					 key += rowval[rind.get(i)];
					 key +="|";
				 }
				if(hashtable.containsKey(key)) {
					
					 ArrayList<String> value = hashtable.get(key);
        			for(String s: value) {
        				//System.out.println(s+"|"+row);
        				joinresult.tuples.add(s+"|"+row);
        				//writeresult(s+"|"+row, joinresult);
        			}
				}
			}
		}
		}
		
		if(ConfigureVariables.mapinsert.containsKey(t2.tableName)) {
			for(String str: ConfigureVariables.mapinsert.get(t2.tableName)) {
				row = str;
				String[] rowval = row.split("\\|");
				/*if(t2.tableName.equalsIgnoreCase("ORDERS")) {
					ConfigureVariables.countcheck = ConfigureVariables.countcheck +1;
				}*/
				if(ConfigureVariables.tableCondition.containsKey(t2.tableName)) {
					
				if(checkSelection(rowval, t2.tableColDef, t2.tableName)) {
					/*if(t2.tableName.equalsIgnoreCase("orders")){
						ConfigureVariables.countcheck = ConfigureVariables.countcheck + 1;
					}*/
					//System.out.println("yes2");
					
					String key="";
					 for(int i =0;i<rind.size();i++) {
						 key += rowval[rind.get(i)];
						 key +="|";
					 }
					
					if(hashtable.containsKey(key)) {
						 ArrayList<String> value = hashtable.get(key);
	         			for(String s: value) {
	         				//System.out.println(s+"|"+row);
	         				joinresult.tuples.add(s+"|"+row);
	         				//writeresult(s+"|"+row, joinresult);
	         			}
					}
				}}
				else {
					//System.out.println("Hello");
					
					String key="";
					 for(int i =0;i<rind.size();i++) {
						 key += rowval[rind.get(i)];
						 key +="|";
					 }
					if(hashtable.containsKey(key)) {
						
						 ArrayList<String> value = hashtable.get(key);
	        			for(String s: value) {
	        				//System.out.println(s+"|"+row);
	        				joinresult.tuples.add(s+"|"+row);
	        				//writeresult(s+"|"+row, joinresult);
	        			}
					}
				}
			}
		}
		
		
		
		//System.out.println(t2.tableName+":"+ConfigureVariables.countcheck);
		//}
		
		}
		
		
		// DO NOT FORGET TO DELETE HASHTABLE FILE AFTER JOIN!!!
		
		//File f = new File(ConfigureVariables.temppath+"hashmap.obj");
	//if(f.exists()) {
	//	f.delete();
	//}
		//System.out.println("End"+joinresult.tableName+"size:"+joinresult.tuples.size());
		return joinresult;
		
}
	
	public static int getcolIndex(ArrayList<String> cn, String col) {
		
		for(int i =0; i<cn.size();i++) {
			//System.out.println(cn.get(i)+":"+col);
			if(col.equals(cn.get(i))) {
				return i;
			}
		}
		
		return 0;
		
	}
	
	public static String getJoinColumn(String t1, String t2) {
		Expression left = null;
		Expression right = null;
		String tbleft = null;
		String tbright =null;
		String lcol = null;
		String rcol=null;
		for(Expression e: ConfigureVariables.joinCondition) {
			left =  (Expression)((BinaryExpression) e).getLeftExpression();
			right = (Expression)((BinaryExpression) e).getRightExpression();
			if(left instanceof Column && right instanceof Column) {
				//System.out.println("Hello");
				tbleft = (String)(((Column) left).getTable().getName());
				tbright = (String)(((Column) right).getTable().getName());
				lcol =(String)(((Column) left).getColumnName());
				rcol = (String)(((Column) right).getColumnName());
				//System.out.println(tbleft+":"+tbright+":"+lcol+":"+rcol);
			}
			if(((t1.equals(tbleft)&& t2.equals(tbright))||((t1.equals(tbright)&& t2.equals(tbleft)))&& lcol.equals(rcol))) {
				return lcol;
			}
		}
		return null;
		
		
	}
	
	public static boolean checkSelection(String[] str, ArrayList<ColumnDefinition> columnDefinitionList, String tableName) {
		//System.out.println("Check sel");
		PrimitiveValue t[];
		
		t = Tuple.getTupleValSplit(str, columnDefinitionList);
		
		Evaluator eval = new Evaluator(t);
		ArrayList<Expression> tc = ConfigureVariables.tableCondition.get(tableName);
		//System.out.println("tc::" + tableName +"::"+ tc);
		//System.exit(0);
		if(tc==null) {
			return true;
		}
		//System.out.println("SIZE:"+tc.size()+tableName+tc);
		if(!tc.isEmpty()) {
			//System.out.println("NON EMPTY");
			int count =0;
			for(Expression condition : tc) {
				try {
					//System.out.println(tableName+":"+condition);
					/*if(tableName.equalsIgnoreCase("ORDERS")) {
						ConfigureVariables.countcheck = ConfigureVariables.countcheck +1;
					}*/
					PrimitiveValue result = eval.eval(condition);
					 BooleanValue boolResult = (BooleanValue)result;
					 if(boolResult.getValue()) {
						 count++;
						 
						 //for( PrimitiveValue v : tuple)
						 //System.out.print(v.toRawString()+'|');
						 //System.out.println();
						 
					 }
					 else {
						 return false;
					 }
					 if(count == tc.size()) {
						 return true;
					 }
					 
				} catch (SQLException e) {
					
					e.printStackTrace();
				}	
			
			}
			
			
		}
		
		else {
			return true;
		}
		
		
		return true; //Default return type ( Not in use).
		
	}

	

	
		
	
	
	
	public static void writeresult(String jrow, MyTable joinresult)  {
		
		FileWriter fw = null;
		BufferedWriter bw =null;
		File f = new File(ConfigureVariables.temppath+joinresult.tableName+ConfigureVariables.FILEEXTENSION);
		if(!f.exists()) {
			try {
				f.createNewFile();
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
		fw = new FileWriter(f.getAbsoluteFile(), true);
		bw = new BufferedWriter(fw);

		bw.write(jrow+"\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally {

				try {

					if (bw != null)
						bw.close();

					if (fw != null)
						fw.close();

				} catch (IOException ex) {

					ex.printStackTrace();

				}
			}
		}
		
public static MyTable CrossProduct(MyTable t1, MyTable t2) {
	
	FileWriter fw =null;
	BufferedWriter bw =null;
	
	
	File file = new File(ConfigureVariables.temppath+t1.tableName+"|"+t2.tableName+ConfigureVariables.FILEEXTENSION);
	if(!file.exists()) {
		try {
			file.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	try {
		fw = new FileWriter(file.getAbsoluteFile(), true);
		bw = new BufferedWriter(fw);

		String row1=null;
		
		while((row1=t1.readtuple())!=null) {
			String tbl1=t1.tableName;
			if(t1.tableName.contains("|")) {
				String tbl[] = t1.tableName.split("\\|");
				tbl1 = tbl[tbl.length-1];
			}
			String rowval1[] = row1.split("\\|");
			if(checkSelection(rowval1, t1.tableColDef, tbl1)) {
			String row2 = null;
			while((row2=t2.readtuple())!=null) {
				String rowval2[] = row2.split("\\|");
				if(checkSelection(rowval2, t2.tableColDef, t2.tableName))
				bw.write(row1+"|"+row2+"\n");
			}
		}
		}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally {

				try {

					if (bw != null)
						bw.close();

					if (fw != null)
						fw.close();

				} catch (IOException ex) {

					ex.printStackTrace();

				}
			}
	
	
	
	return null;
}

}

 class joinSpec{
	String jcl;
	String jcr;
}


