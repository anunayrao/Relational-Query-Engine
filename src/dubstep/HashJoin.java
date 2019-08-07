package dubstep;
////@Author - Anunay Rao
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class HashJoin {

	public static MyTable computeJoin(MyTable t1, MyTable t2) {
		//System.out.println(t1.tableName + t2.tableName);
		String joinColumn = null;
		String jcl=null;
		String jcr=null;
		
		//System.out.println(t1.tableName + t2.tableName);
		if(t1.tableName.contains("|")) {
			String tblname[] = t1.tableName.split("\\|");
			
			String tableName1 = tblname[tblname.length -1];
			// Can be modified to check for all the join conditions for each table;
			for(int i =0; i<tblname.length;i++) {
				//System.out.println(tblname[i]);
				joinColumn = getJoinColumn(tblname[i], t2.tableName);
				if(joinColumn!=null) {
					jcl = tblname[i]+"."+joinColumn;
					jcr = t2.tableName+"."+joinColumn;
					//break;		
				}
			}
			//joinColumn = getJoinColumn(tableName1, t2.tableName);
		}
		else {
			joinColumn = getJoinColumn(t1.tableName, t2.tableName);	
			jcl = t1.tableName+"."+joinColumn;
			jcr = t2.tableName+"."+joinColumn;
		}
		
		//System.out.println(joinColumn);
		
		ArrayList<ColumnDefinition> jointablecd = new ArrayList();
		for(ColumnDefinition cd : t1.tableColDef) {
			jointablecd.add(cd);
		}
		for(ColumnDefinition cd : t2.tableColDef) {
			jointablecd.add(cd);
		}
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
	        
	        joinresult.columnDatatype = cd;
	        joinresult.columnNames =cn;
		HashMap<String,ArrayList<String>> hashtable = new HashMap();
		
		
		if(joinColumn==null) {
			CrossProduct(t1, t2);
			String r;
			/*
			while((r=joinresult.readtuple())!=null) {
				//System.out.println(r);
			}*/
			return joinresult;
			
			//System.out.println("No join Condition");
		}
		int l,r;
		l = getcolIndex(t1.columnNames, jcl);
		r = getcolIndex(t2.columnNames,jcr);
		
		
		
		//System.out.println(l+":"+r);
		
		int hashobjectsize = 10000;
		String row=null;
		int count =0;
		
		while((row=t1.readtuple())!=null) {
			//System.out.println("While");
			if(checkSelection(row, t1.tableColDef,t1.tableName)) {
			
			//System.out.println("Yes");
			if(count<hashobjectsize) {
				String[] rowval = row.split("\\|");
				if(hashtable.containsKey(rowval[l])) {
					hashtable.get(rowval[l]).add(row);
					count++;
				}
				else {
					ArrayList<String> temp = new ArrayList();
					temp.add(row);
					hashtable.put(rowval[l],temp);
					count++;
				}
				
			}
			else {
				
				try {
					writetofile(hashtable);
					//System.out.println("Written");
					hashtable.clear();
					count =0;
					String[] rowval = row.split("\\|");
					ArrayList<String> temp = new ArrayList();
					temp.add(row);
					hashtable.put(rowval[l],temp);
					count++;
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//Write Object to File and Clear the hashtable also add support for selection pushdown
				//in above if condition also set count =0;
			}
			
			
		}
		}
		if(!hashtable.isEmpty()) {
			try {
				writetofile(hashtable);
				//System.out.println("yyyWritten");
				hashtable.clear();
				count =0;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		
		
		
		
		try {
			readFile(t2,r,joinresult);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		// DO NOT FORGET TO DELETE HASHTABLE FILE AFTER JOIN!!!
		
		File f = new File(ConfigureVariables.temppath+"hashmap.obj");
	if(f.exists()) {
		f.delete();
	}
		return joinresult;
		
	}
	
	public static int getcolIndex(ArrayList<String> cn, String col) {
		
		for(int i =0; i<cn.size();i++) {
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
		//System.out.println("Tables to compare:"+t1 +":----:"+t2);
		for(Expression e: ConfigureVariables.joinCondition) {
			//System.out.println("Expression:"+e.toString());
			left =  (Expression)((BinaryExpression) e).getLeftExpression();
			right = (Expression)((BinaryExpression) e).getRightExpression();
			//System.out.println("LEFT:"+left + "RIGHT:"+right);
			if(left instanceof Column && right instanceof Column) {
				//System.out.println("Hello");
				tbleft = (String)(((Column) left).getTable().getName());
				tbright = (String)(((Column) right).getTable().getName());
				lcol =(String)(((Column) left).getColumnName());
				rcol = (String)(((Column) right).getColumnName());
				//System.out.println("YAA:"+tbleft+":"+tbright+":"+lcol+":"+rcol);
			}
			if(((t1.equals(tbleft)&& t2.equals(tbright))||((t1.equals(tbright)&& t2.equals(tbleft)))&& lcol.equals(rcol))) {
				return lcol;
			}
		}
		return null;
		
		
	}
	
	public static boolean checkSelection(String str, ArrayList<ColumnDefinition> columnDefinitionList, String tableName) {
		//System.out.println("Check sel");
		PrimitiveValue t[];
		t = Tuple.getTupleVal(str, columnDefinitionList);
		
		Evaluator eval = new Evaluator(t);
		ArrayList<Expression> tc = ConfigureVariables.tableCondition.get(tableName);
		if(tc==null) {
			return true;
		}
		//System.out.println("SIZE:"+tc.size()+tableName+tc);
		if(!tc.isEmpty()) {
			//System.out.println("NON EMPTY");
			int count =0;
			for(Expression condition : tc) {
				try {
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

	public static void writetofile(HashMap<String,ArrayList<String>> hmap) throws IOException {
		boolean exists = new File(ConfigureVariables.temppath+"hashmap.obj").exists();
	    FileOutputStream fos;
		try {
			fos = new FileOutputStream(ConfigureVariables.temppath+"hashmap.obj", true);
		
	    ObjectOutputStream oos = exists ? 
	        new ObjectOutputStream(fos) {
	            protected void writeStreamHeader() throws IOException {
	                reset();
	            }
	        }:new ObjectOutputStream(fos);
	        
	        oos.writeObject(hmap);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
		
	public static void readFile(MyTable t2, int colindex, MyTable joinresult) throws IOException {
		HashMap<String, ArrayList<String>> map = null;
	    //HashMap<Integer, String> map1 = null;
	    try
	    { 
	    FileInputStream fis = new FileInputStream(ConfigureVariables.temppath+"hashmap.obj");
	    ObjectInputStream ois = new ObjectInputStream(fis);
	    	
	    	while(fis.available() > 0) {
	        try {
	            map = (HashMap)ois.readObject();
	            /* 
	            Set set = map.entrySet();
	             Iterator iterator = set.iterator();
	             while(iterator.hasNext()) {
	                Map.Entry mentry = (Map.Entry)iterator.next();
	               // System.out.print("key: "+ mentry.getKey() + " & Value: ");
	                String key = (String) mentry.getKey();
	                ArrayList<String> value = (ArrayList<String>) mentry.getValue();
	               // System.out.println(mentry.getValue());
	               // System.out.println(key +":"+value);
	             } */
	            
	            String row = null;
	               while((row=t2.readtuple())!=null) {
	            	   if(checkSelection(row, t2.tableColDef, t2.tableName)) {
	            		   String[] rowval = row.split("\\|");
	            		   
	            		   
	            		   if(map.containsKey(rowval[colindex])) {
	            			  ArrayList<String> value = map.get(rowval[colindex]);
	            			for(String s: value) {
	            				//System.out.println(s+"|"+row);
	            				writeresult(s+"|"+row, joinresult);
	            			}
	         	 
	       				}
	       				
	            		   
	            	   }
	               }
	        
	        
	        } catch (ClassNotFoundException e) {
	            e.printStackTrace();
	        }
	    }
	    	ois.close();
	    	
	    }catch(FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
				String tbl[] = t1.tableName.split("//|");
				tbl1 = tbl[tbl.length-1];
			}
			if(checkSelection(row1, t1.tableColDef, tbl1)) {
			String row2 = null;
			while((row2=t2.readtuple())!=null) {
				if(checkSelection(row2, t2.tableColDef, t2.tableName))
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




