package dubstep;
import net.sf.jsqlparser.expression.BinaryExpression;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
////@Author - Anunay Rao,Apoorva Biseria
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.update.Update;


public class Main {
	
	public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException {
		
		
		//Pull Method for Reading Table
		
		/*Table t = new Table("Q");
		String read;
		while((read = t.readtuple())!=null) {
			System.out.println(read);
		}*/
		ConfigureVariables.starttime = System.currentTimeMillis();
		File tempfolder = new File(ConfigureVariables.temppath);
		tempfolder.mkdir();
		for(int i=0;i<args.length;i++) {
			if(args[i].equals("--in-mem")) {
				ConfigureVariables.inmem =true;
				//ConfigureVariables.ondisk=true;
				break;
			}
			if(args[i].equals("--on-disk")) {
				ConfigureVariables.ondisk = true;
				break;
			}
		}
		
		if(ConfigureVariables.index) {
		
		HashMap<String,String> r = new HashMap<>();
		
		r = readMap("stats");
		/*
		for (String s : r.keySet()) {
			System.out.println(s);
			System.out.println(r.get(s));
		}
		*/
		if(r!=null) {
			//System.out.println("Called");
			getStats(r);
		}
		/*
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date1 = null;
        Date date2=null;
		try {
			 date1 = sdf.parse("2009-12-31");
			 date2 = sdf.parse("2010-01-31");
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        ArrayList<Integer> test = new ArrayList<Integer>();
        test.add(1);
        test.add(0);
		NavigableMap<Date, ArrayList<Integer>> nmap = new TreeMap<Date, ArrayList<Integer>>();
		
		nmap.put(date1, test);
		nmap.put(date2,test);
		try {
			nmap.put(sdf.parse("1999-10-10"),test);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			nmap.put(sdf.parse("2020-10-21"),test);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.printf("Original Map : %s%n", nmap);
		//writenmap( nmap, "nmap");
		
		NavigableMap<Date, ArrayList<String>> nm = new TreeMap<Date, ArrayList<String>>();
		nm = readNMap("nmap");
		System.out.println(nm);
		
		*/
		
		
		/*
		r = readMap("tableObjects");
		if(r!=null) {
			ConfigureVariables.tableObjects=r;
		}
		
		
		r = readMap("createTableMap");
		if(r!=null) {
			ConfigureVariables.createTableMap = r;
		}
		*/
		
		Date startdate,enddate;
		NavigableMap<Date, ArrayList<Long>> nm;
		NavigableMap<Date, ArrayList<Long>> hnm = null;;
		//Expression e = ConfigureVariables.tableCondition.get("ORDERS").get(0);
		//String sidate = ((BinaryExpression)e).getRightExpression().toString();
		//String sdate = sidate.substring(6, 16);
		//Expression e1 = ConfigureVariables.tableCondition.get("ORDERS").get(1);
		//String eidate = ((BinaryExpression)e1).getRightExpression().toString();
		//String edate = sidate.substring(6, 16);
		/*
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			startdate = sdf.parse("1993-01-01");
			enddate = sdf.parse("1994-01-01");
			 nm = Main.readNMap("ORDERS.ind");
			 hnm = nm.subMap(startdate, true, enddate, false);
			 ArrayList<Long> set = new ArrayList<Long>();
				for(Date d : hnm.keySet()) {
					ArrayList<Long> bytes = hnm.get(d);
					set.addAll(bytes);
				}
				ArrayList<Long> oset = new ArrayList<Long>();
				for(Date d : nm.keySet()) {
					ArrayList<Long> bytes = nm.get(d);
					oset.addAll(bytes);
				}
				System.out.println("Original size:"+oset.size());
				Collections.sort(set);
				System.out.println(set.size());
				System.out.println(set.get(set.size()-1));
				System.out.println(set.get(0));
				System.out.println(set.get(1));
				System.out.println(set.get(2));
		} catch (ClassNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (java.text.ParseException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		*/
		
		FileReader fr;
		BufferedReader br;
		try {
			fr = new FileReader(ConfigureVariables.pathname+"ORDERS"+ConfigureVariables.FILEEXTENSION);
			 br = new BufferedReader(fr);
		
		
		
		
	
		//System.exit(0);
		int skip =0;
		//int count =0;
		//int r =0;
		/*
		for(Long i : set) {
			//System.out.println(i);
			//System.out.println(skip);
			br.skip(i-skip);
			String s = br.readLine();
			if(s==null) {
				break;
			}
		}*/
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		
		
		
		}
		
		
		
		
		
		
		
		System.out.println(ConfigureVariables.PROMPT);
		System.out.flush();
		
		Reader input = new InputStreamReader(System.in);
		CCJSqlParser parser = new CCJSqlParser(input);
		
		Statement statement ;
		MainHandler sqlParserMethod = new MainHandler();
		/*
		HashMap<String,ArrayList<String>> hmap = new HashMap();
		HashMap<String,String> str = new HashMap<String, String>();
		str.put("NAMES","AHAANA");
		ArrayList<String> test = new ArrayList<String>();
		test.add("ANUNAY");
		test.add("Apoorva");
		hmap.put("NAMES",test);
		writemap(hmap, "tablecol");
		HashMap h = readMap("tablecol");
		
		System.out.println(h.get("NAMES"));
		
		writemap(str, "names");
		HashMap he = readMap("names");
		System.out.println(he.get("NAMES"));
		*/
		while((statement=parser.Statement()) != null) {
			
			if (statement instanceof CreateTable) {
                CreateTable createTable = (CreateTable)statement;
                sqlParserMethod.createTable(createTable);
                
        		//input= new InputStreamReader(System.in);
        		
                
            }
			else if (statement instanceof Select) {
                
				SelectBody selectBody = ((Select)statement).getSelectBody();

                if(selectBody instanceof Union) {
                    
                    
                    Union union =(Union)selectBody;
            		//List<PlainSelect> plainSelects =union.getPlainSelects();
            		MainHandler.unionStatement(union);
                    //System.out.println(plainSelects.get(0));
            		//System.out.println(plainSelects.get(1));
            		/*
            		for(PlainSelect i:plainSelects) {
            			SQLParserMethod.plainStatement(i);
            		}
            		*/
            		//System.out.println("UNION");
                    //System.out.print(ConfigureVariables.PROMPT);
        			//System.out.flush();
        			//System.gc();	
            		
		}
			
			else {
				
				//System.out.println("Plain Select Statement");
				PlainSelect plain = (PlainSelect)selectBody;
				MainHandler.plainStatement(plain);
				
			}
		}
			else if(statement instanceof Delete) {
				
				ConfigureVariables.delete = true;
				Delete stmt = (Delete)statement;
				String tname = stmt.getTable().getName();
				ConfigureVariables.tableStrings.add(tname);
				Expression expression = stmt.getWhere();
				//InverseExpression inverse = new InverseExpression(expression);
				ConfigureVariables.whereArrayList.add(expression);
				//System.out.println(ConfigureVariables.whereArrayList);
				MyTable table = ConfigureVariables.tableObjects.get(tname.toUpperCase());
				TableScan.SimpleScan(table, ConfigureVariables.tablefile, ConfigureVariables.tableStrings);
				/*
				for(int i=0;i<ConfigureVariables.afterDelete.size();i++) {
					System.out.println(ConfigureVariables.afterDelete.get(i));
				}
				*/
				//Write down the afterdelete tuples to the table file. 
				
				/*
				FileWriter fw = new FileWriter(ConfigureVariables.pathname+tname+ConfigureVariables.FILEEXTENSION);
				BufferedWriter bw = new BufferedWriter(fw);
				for(int i=0;i<ConfigureVariables.afterDelete.size();i++) {
					bw.write(ConfigureVariables.afterDelete.get(i)+ "\n");
				}
				
				bw.close();
				*/
				//System.out.println(ConfigureVariables.afterDelete);
				
				if(ConfigureVariables.mapdelete.containsKey(tname)) {
					ArrayList<String> values = ConfigureVariables.mapdelete.get(tname);
					for(int i=0;i<ConfigureVariables.afterDelete.size();i++) {
						if(!values.contains(ConfigureVariables.afterDelete.get(i))) {
							values.add(ConfigureVariables.afterDelete.get(i));
						}
					}
					//values.addAll(ConfigureVariables.afterDelete);
					ConfigureVariables.mapdelete.put(tname, values);
					//ConfigureVariables.mapdelete.put(tname,ConfigureVariables.afterDelete);
				}
				else {
					//ArrayList<String> values = new ArrayList<String>();
					ConfigureVariables.mapdelete.put(tname, ConfigureVariables.afterDelete);
					//System.out.println("Done");
				}
				//ConfigureVariables.deletereset();
				
				
				if(ConfigureVariables.mapinsert.containsKey(tname)) {
					ArrayList<String> deleted = ConfigureVariables.mapdelete.get(tname);
					ArrayList<String> newval = ConfigureVariables.mapinsert.get(tname);
					for(String s : deleted) {
						if(newval.contains(s)) {
							newval.remove(s);
						}
					}
					ConfigureVariables.mapinsert.put(tname,newval);
				}
				
				/*
				for(String k : ConfigureVariables.mapdelete.keySet()) {
					System.out.println("Deleted:"+k +":"+ ConfigureVariables.mapdelete.get(k)  );
				}
				for(String k : ConfigureVariables.mapinsert.keySet()) {
					System.out.println("Inserted:"+k +":"+ ConfigureVariables.mapinsert.get(k)  );
				}*/
				
				ConfigureVariables.reset();
				
			}
			else if(statement instanceof Insert) {
				
				Insert stmt = (Insert)statement;
				String tname = stmt.getTable().getName();
				List insertItems = ((ExpressionList) stmt.getItemsList()).getExpressions();
				//System.out.println(insertItems);
				String insert="";
				for(Object o : insertItems) {
					String item = o.toString();
					if(item.contains("'")) {
						item = item.substring(item.indexOf("'") + 1, item.lastIndexOf("'"));
					}
					
					insert += item +"|";
				}
				
				insert = insert.substring(0, insert.lastIndexOf("|"));
				
				if(ConfigureVariables.mapdelete.containsKey(tname)) {
					ArrayList<String> newval = ConfigureVariables.mapdelete.get(tname);
					if(newval.contains(insert)) {
						newval.remove(insert);
					}
					ConfigureVariables.mapdelete.put(tname,newval);
				}
				
				 if(ConfigureVariables.mapinsert.containsKey(tname)) {
					ArrayList<String> values = ConfigureVariables.mapinsert.get(tname);
					values.add(insert);
					ConfigureVariables.mapinsert.put(tname,values);	
				}
				else {
					ArrayList<String> values = new ArrayList<String>();
					values.add(insert);
					ConfigureVariables.mapinsert.put(tname,values);
					
				}
				 
				/* for(String k : ConfigureVariables.mapdelete.keySet()) {
						System.out.println("Deleted:"+k +":"+ ConfigureVariables.mapdelete.get(k)  );
					}
					for(String k : ConfigureVariables.mapinsert.keySet()) {
						System.out.println("Inserted:"+k +":"+ ConfigureVariables.mapinsert.get(k)  );
					}*/
				
				//System.out.println(insert);
				/* Read Only File System
				FileWriter fw = new FileWriter(ConfigureVariables.pathname+tname+ConfigureVariables.FILEEXTENSION,true);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(insert+"\n");
				bw.close();
				*/
				
			}
			else if(statement instanceof Update) {
				/*
				Insert stmt = (Insert)statement;
				String tname = stmt.getTable().getName();
				List insertItems = ((ExpressionList) stmt.getItemsList()).getExpressions();
				//System.out.println(insertItems);
				String insert="";
				for(Object o : insertItems) {
					String item = o.toString();
					if(item.contains("'")) {
						item = item.substring(item.indexOf("'") + 1, item.lastIndexOf("'"));
					}
					
					insert += item +"|";
				}
				
				insert = insert.substring(0, insert.lastIndexOf("|"));
				//System.out.println(insert);
				FileWriter fw = new FileWriter(ConfigureVariables.pathname+tname+ConfigureVariables.FILEEXTENSION,true);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(insert+"\n");
				bw.close();
				*/
			}
			
		
		System.out.println(ConfigureVariables.PROMPT);
    	System.out.flush();
    	//System.gc();
    	
    	input = new InputStreamReader(System.in);
    	parser = new CCJSqlParser(input);
    	
    	
    	/*
		System.out.println("Create Table Map");
		for (Map.Entry<String, CreateTable> entry : sqlParserMethod.createTableMap.entrySet())
		{
			String s = entry.getKey();
			CreateTable c = entry.getValue();
			System.out.println(s +" : "+ c );
		}
		System.out.println("****************");
		*/
		
					
				}
		
			}
	
	
	
	
	


public static NavigableMap readNMap(String name) throws IOException, ClassNotFoundException  {
		
		//FileInputStream fis = new FileInputStream(ConfigureVariables.temppath+"hashmap.obj");
	    //ObjectInputStream ois = new ObjectInputStream(fis);
		NavigableMap h = new TreeMap();
		FileInputStream fis =null;
		ObjectInputStream ois  = null;
		FileReader fr = null;
		BufferedReader br =null;
		File f = new File(ConfigureVariables.temppath+name);
		if(f.exists()) {
			//System.out.println("Exists!!!1");
			fis = new FileInputStream(ConfigureVariables.temppath+name);
			BufferedInputStream bis = new BufferedInputStream(fis);
			ois  = new ObjectInputStream(bis);
			h = (NavigableMap)ois.readObject();
			return h;
			
		}
		return null;
		
		}
	
	public static HashMap readMap(String name) throws IOException, ClassNotFoundException  {
		
		//FileInputStream fis = new FileInputStream(ConfigureVariables.temppath+"hashmap.obj");
	    //ObjectInputStream ois = new ObjectInputStream(fis);
		HashMap h = new HashMap<>();
		FileInputStream fis =null;
		ObjectInputStream ois  = null;
		FileReader fr = null;
		BufferedReader br =null;
		File f = new File(ConfigureVariables.temppath+name);
		if(f.exists()) {
			//System.out.println("Exists!!!1");
			fis = new FileInputStream(ConfigureVariables.temppath+name);
			BufferedInputStream bis = new BufferedInputStream(fis);
			ois  = new ObjectInputStream(bis);
			h = (HashMap) ois.readObject();
			return h;
			
		}
		return null;
		
		}
public static void writenmap(NavigableMap<Date, ArrayList<Long>> nmap,String f) throws IOException {
		
		
	    FileOutputStream fos;
		try {
			fos = new FileOutputStream(ConfigureVariables.temppath+f);
		
	    ObjectOutputStream oos = new ObjectOutputStream(fos);
	        
	        oos.writeObject(nmap);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
public static void writenmapL(NavigableMap<Date, ArrayList<Integer>> nmap,String f) throws IOException {
	
	
    FileOutputStream fos;
	try {
		fos = new FileOutputStream(ConfigureVariables.temppath+f);
	BufferedOutputStream bos = new BufferedOutputStream(fos);
    ObjectOutputStream oos = new ObjectOutputStream(bos);
       
        oos.writeObject(nmap);
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
	
	public static void getStats(HashMap<String,String> h) {
		//System.out.println("Hello");
		for(String key : h.keySet()) {
			//System.out.println("yuppp");
			String tname = key;
			String stmt = h.get(key);
			Table t = new Table();
			t.setName(tname.toUpperCase());
			CreateTable c = new CreateTable();
			c.setTable(t);
			
			String ele[] = stmt.split("#");
			ArrayList<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
			for(int i=0;i<ele.length;i++) {
				String nd[] = ele[i].split(" ");
				ColumnDefinition col = new ColumnDefinition();
				col.setColumnName(nd[0]);
				ColDataType cdt =new ColDataType();
				cdt.setDataType(nd[1]);
				col.setColDataType(cdt);
				columns.add(col);
			}
			c.setColumnDefinitions(columns);
			
			//System.out.println("hello");
			//Creating Table Object
			String tableName = c.getTable().getName().toLowerCase();
			//System.out.println("getstats"+tableName);
			ArrayList<ColumnDefinition> columnDefinitionList = (ArrayList<ColumnDefinition>) c.getColumnDefinitions();
			//System.out.println(columnDefinitionList+"yooooo");
			MyTable newTable = new MyTable(tableName.toUpperCase(), columns,true);
		        //newTable.colDef = columnDefinitionList2;
		       // System.out.println("H:"+newTable.colDef);
		        ArrayList<String> cn = new ArrayList<String>();
		        ArrayList<ColDataType> cd = new ArrayList<ColDataType>();
		        for(int i=0;i<columnDefinitionList.size();i++) {
		        	//System.out.println(columnDefinitionList.size());
		        	cn.add(tableName.toUpperCase()+"."+columnDefinitionList.get(i).getColumnName().toString());
		        	cd.add(columnDefinitionList.get(i).getColDataType());
		        }
		        //System.out.println(cn);
		        //System.out.println(cd);
		        newTable.columnDatatype = cd;
		        newTable.columnNames =cn;
		        //System.out.println(newTable.columnDatatype);
		        //System.out.println(newTable.columnNames);
		        ConfigureVariables.tableObjects.put(tableName.toUpperCase(), newTable);
			
		        
		        ///Tablecol
		        
		        ArrayList<String> col = new ArrayList<String>();
		        HashMap<String, ColumnInfo> tableMap = new HashMap<>();
		        for (int i = 0; i < columnDefinitionList.size(); i++) {
		            String colName = columnDefinitionList.get(i).getColumnName().toLowerCase();
		            ColDataType colDataType = columnDefinitionList.get(i).getColDataType();
		            ColumnInfo columnInfo = new ColumnInfo(i, colDataType.getDataType());
		            tableMap.put(tableName+"."+colName, columnInfo);
		            //ConfigureVariables.tablecol.put(tableName,colName);
		            col.add(colName);
		            
		        }
		       
		        ConfigureVariables.tablecol.put(tableName,col);
			
		}
	}
	
	public static void BuildIndexNMP2(MyTable table,int index) throws java.text.ParseException, IOException {
	
		
		//RandomAccessFile  r = new RandomAccessFile(, "r");
		NavigableMap<Date, ArrayList<Long>> nmap = new TreeMap<Date, ArrayList<Long>>();
		String row=null;
		FileReader  fread = new FileReader(ConfigureVariables.pathname+table.tableName+ConfigureVariables.FILEEXTENSION);
		BufferedReader brr = new BufferedReader(fread);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		long pbytes = 0;
		ArrayList<Long> all = new ArrayList<Long>();
		while((row=brr.readLine())!=null) {
		Date date=null;
		long bytes;
		
		String rowval[] = row.split("\\|");
		date = sdf.parse(rowval[index]);
		//System.out.println(rowval[index]);
		if(nmap.isEmpty()) {
			//System.out.println("yes");
			ArrayList<Long> t = new ArrayList<Long>();
			pbytes = 0L;
			t.add(pbytes);
			all.add(pbytes);
			nmap.put(date,t);
			pbytes = (row.getBytes().length +1) + pbytes;
			//System.out.println(pbytes);
			//break;
		}
		else {
			if(nmap.containsKey(date)) {
				ArrayList<Long> t = nmap.get(date);
				t.add(pbytes);
				all.add(pbytes);
				nmap.put(date,t);
				pbytes = (row.getBytes().length +1) + pbytes;
				//pbytes = bytes;
				
				
			}
			else {
				ArrayList<Long> t = new ArrayList<Long>();
				t.add(pbytes);
				all.add(pbytes);
				nmap.put(date,t);
				pbytes = row.getBytes().length + 1 + pbytes;
				//pbytes = bytes;
				
				
			}
			
		}
		
		//System.out.println(pbytes);
		}
		Collections.sort(all);
		//System.out.println(all.get(all.size()-1));
		
		
		writenmap(nmap, table.tableName+"R.ind");
	}
	
public static void BuildIndexNMP(MyTable table,int index) throws java.text.ParseException, IOException {
	
		
		//RandomAccessFile  r = new RandomAccessFile(, "r");
		NavigableMap<Date, ArrayList<Long>> nmap = new TreeMap<Date, ArrayList<Long>>();
		String row=null;
		FileReader  fread = new FileReader(ConfigureVariables.pathname+table.tableName+ConfigureVariables.FILEEXTENSION);
		BufferedReader brr = new BufferedReader(fread);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		long pbytes = 0;
		ArrayList<Long> all = new ArrayList<Long>();
		while((row=brr.readLine())!=null) {
		Date date=null;
		long bytes;
		
		String rowval[] = row.split("\\|");
		date = sdf.parse(rowval[index]);
		//System.out.println(rowval[index]);
		if(nmap.isEmpty()) {
			//System.out.println("yes");
			ArrayList<Long> t = new ArrayList<Long>();
			pbytes = 0L;
			t.add(pbytes);
			all.add(pbytes);
			nmap.put(date,t);
			pbytes = (row.getBytes().length +1) + pbytes;
			//System.out.println(pbytes);
			//break;
		}
		else {
			if(nmap.containsKey(date)) {
				ArrayList<Long> t = nmap.get(date);
				t.add(pbytes);
				all.add(pbytes);
				nmap.put(date,t);
				pbytes = (row.getBytes().length +1) + pbytes;
				//pbytes = bytes;
				
				
			}
			else {
				ArrayList<Long> t = new ArrayList<Long>();
				t.add(pbytes);
				all.add(pbytes);
				nmap.put(date,t);
				pbytes = row.getBytes().length + 1 + pbytes;
				//pbytes = bytes;
				
				
			}
			
		}
		
		//System.out.println(pbytes);
		}
		Collections.sort(all);
		//System.out.println(all.get(all.size()-1));
		
		
		writenmap(nmap, table.tableName+".ind");
	}

public static void BuildIndexNMPL(MyTable table,int index) throws java.text.ParseException, IOException {
	
		
		//RandomAccessFile  r = new RandomAccessFile(, "r");
		NavigableMap<Date, ArrayList<Integer>> nmap = new TreeMap<Date, ArrayList<Integer>>();
		String row=null;
		FileReader  fread = new FileReader(ConfigureVariables.pathname+table.tableName+ConfigureVariables.FILEEXTENSION);
		BufferedReader brr = new BufferedReader(fread);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		int pbytes = 0;
		while((row=brr.readLine())!=null) {
		Date date=null;
		
		
		String rowval[] = row.split("\\|");
		date = sdf.parse(rowval[index]);
		if(nmap.isEmpty()) {
			ArrayList<Integer> t = new ArrayList<Integer>();
			pbytes = 0;
			t.add(pbytes);
			nmap.put(date,t);
			pbytes =  pbytes + 1;
			//break;
		}
		else {
			if(nmap.containsKey(date)) {
				ArrayList<Integer> t = nmap.get(date);
			
				t.add(pbytes);
				nmap.put(date,t);
				pbytes = pbytes +1;
				
			}
			else {
				ArrayList<Integer> t = new ArrayList<Integer>();
				t.add(pbytes);
				nmap.put(date,t);
				pbytes = pbytes+1;
				
			}
			
		}
		
		//System.out.println(pbytes);
		}
		
		writenmapL(nmap, table.tableName+".ind");
	}
	
	public static void BuildIndexHMP(MyTable table, int index) throws IOException {
		
		HashMap<String,ArrayList<Long>> hmap = new HashMap<String, ArrayList<Long>>();
		String row=null;
		FileReader  fread = new FileReader(ConfigureVariables.pathname+table.tableName+ConfigureVariables.FILEEXTENSION);
		BufferedReader brr = new BufferedReader(fread);
		
		long pbytes = 0;
		while((row=brr.readLine())!=null) {
		
		long bytes;
		
		String rowval[] = row.split("\\|");
		String key = rowval[index];
		if(hmap.isEmpty()) {
			ArrayList<Long> t = new ArrayList<Long>();
			pbytes = 0L;
			t.add(pbytes);
			hmap.put(key,t);
			pbytes = (row.getBytes().length +1) + pbytes;
			//break;
		}
		else {
			if(hmap.containsKey(key)) {
				ArrayList<Long> t = hmap.get(key);
				t.add(pbytes);
				hmap.put(key,t);
				bytes = (row.getBytes().length +1) + pbytes;
				pbytes = bytes;
				
			}
			else {
				ArrayList<Long> t = new ArrayList<Long>();
				t.add(pbytes);
				hmap.put(key,t);
				bytes = row.getBytes().length + 1 + pbytes;
				pbytes = bytes;
			}
			
		}
		//System.out.println(pbytes);
		}
		writemap(hmap,table.tableName);
	}
	
	
	
public static void writemap(HashMap<String, ArrayList<Long>> hmap,String f) throws IOException {
		
		
	    FileOutputStream fos;
		try {
			fos = new FileOutputStream(ConfigureVariables.temppath+f+".ind");
			//BufferedOutputStream bos = new BufferedOutputStream(fos);
	    ObjectOutputStream oos = new ObjectOutputStream(fos);
	        
	        oos.writeObject(hmap);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

public static void writemapList(HashMap<String,ArrayList<Long>> hmap,String f) throws IOException {
	
	
    FileOutputStream fos;
	try {
		fos = new FileOutputStream(ConfigureVariables.temppath+f+".ind");
		//BufferedOutputStream bos = new BufferedOutputStream(fos);
    ObjectOutputStream oos = new ObjectOutputStream(fos);
        
        oos.writeObject(hmap);
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
public static HashMap readMapList(String name) throws IOException, ClassNotFoundException  {
	
	//FileInputStream fis = new FileInputStream(ConfigureVariables.temppath+"hashmap.obj");
    //ObjectInputStream ois = new ObjectInputStream(fis);
	HashMap <String,ArrayList<Long>> h = new HashMap<>();
	FileInputStream fis =null;
	ObjectInputStream ois  = null;
	FileReader fr = null;
	BufferedReader br =null;
	File f = new File(ConfigureVariables.temppath+name);
	if(f.exists()) {
		//System.out.println("Exists!!!1");
		fis = new FileInputStream(ConfigureVariables.temppath+name);
		BufferedInputStream bis = new BufferedInputStream(fis);
		ois  = new ObjectInputStream(bis);
		h = (HashMap<String,ArrayList<Long>>) ois.readObject();
		return h;
		
	}
	return null;
	
	}
			

	
}
