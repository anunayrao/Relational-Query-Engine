package dubstep;
////@Author - Apoorva Biseria and Anunay Rao
import net.sf.jsqlparser.expression.BinaryExpression;
//@//@Author - Apoorva Biseria
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

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
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

import dubstep.ColumnInfo;
import dubstep.ConfigureVariables;

public class MainHandler  {

	//static HashMap<String, HashMap<String, ColumnInfo>> dbMap = new HashMap<String, HashMap<String,ColumnInfo>>();
	//static HashMap<String, CreateTable> createTableMap = new HashMap<>();
	//HashMap<String, BufferedReader> tablefile = new HashMap<String,BufferedReader>();
    
	public void createTable(CreateTable createTable) {
		
    	String tableName = createTable.getTable().getName().toLowerCase();
    	
    	String stmt = createTable.toString();
    	//System.out.println(stmt.split("PRIMARY")[0]);
    	
   
    	
    	
    	
    	
    	//ConfigureVariables.stats.put(n,stmt);
    	//ConfigureVariable.statsnum.put(n,l);
    	
    	/*BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(ConfigureVariables.pathname + tableName + ".dat"));
			ConfigureVariables.tablefile.put(tableName,in);
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
		*/
    	
    	
    	String cde ="";
        ArrayList<ColumnDefinition> columnDefinitionList = (ArrayList<ColumnDefinition>) createTable.getColumnDefinitions();
        
        if(ConfigureVariables.index) {
        for(int i=0; i<columnDefinitionList.size();i++) {
        	cde += columnDefinitionList.get(i);
        	cde +="#";
        }
        
        ConfigureVariables.stats.put(tableName,cde);
        
        try {
			writemap(ConfigureVariables.stats, "stats");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	}
		
		
        //System.out.println(cde);
        //System.out.println(columnDefinitionList.size());
        /*
        for(int i =0; i<columnDefinitionList.size();i++) {
        	columnDefinitionList.get(i).setColumnName(tableName.toUpperCase()+"."+columnDefinitionList.get(i).getColumnName());
        }
        */
        //System.out.println(columnDefinitionList);
        
        MyTable newTable = new MyTable(tableName.toUpperCase(), columnDefinitionList,true);
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
       //System.out.println(columnDefinitionList);
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
        
        /*
        System.out.println(" Table Map");
        for (Map.Entry<String, ColumnInfo> entry : tableMap.entrySet())
		{
			String s = entry.getKey();
			ColumnInfo c = entry.getValue();
			
			System.out.println(s +" : "+ c.colNo + "," + c.colDataType );
		}
		System.out.println("****************");
		System.out.flush();
        */
        ConfigureVariables.tablecol.put(tableName,col);
		//ConfigureVariables.dbMap.put(tableName, tableMap);
		
        ConfigureVariables.createTableMap.put(tableName, createTable);
        //System.out.println(createTable.get("n"));
        //PrimitiveValue []t;
        //String line = "C|ANUNAY|RAO";
        //t = Tuple.getTuple(line, ConfigureVariables.createTableMap,ConfigureVariables.tableStrings);
		
        //System.out.println("Tuple:"+ t[1]);
        /*
        try {
			//writemap(ConfigureVariables.tablecol,"tablecol");
			//writemap(ConfigureVariables.tableObjects,"tableObjects");
			writemap(ConfigureVariables.createTableMap,"createTableMap");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        */
        if(ConfigureVariables.index) {  //Remove the inside if condition false or change it to true to enable indexing
        if(false) {
        if(newTable.tableName.equalsIgnoreCase("lineitem")){
        	
        	try {
        		//long startTime = System.currentTimeMillis();
				Main.BuildIndexNMP(newTable, 10);
				Main.BuildIndexNMP2(newTable,12);
				//long endTime = System.currentTimeMillis();

				//long timeElapsed1 = endTime - startTime;

				//System.out.println("Execution time in milliseconds for Index Creation : " + timeElapsed1);
			} catch (ParseException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        	/*
        	NavigableMap<Date, ArrayList<String>> nm = new TreeMap<Date, ArrayList<String>>();
    		try {
				nm = Main.readNMap(newTable.tableName+".ind");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
    		//System.out.println(nm);
    		//System.out.println(nm.size());
        }
        if(true){
         if(newTable.tableName.equalsIgnoreCase("orders")) { //else if 
        	try {
        		//long startTime = System.currentTimeMillis();
				Main.BuildIndexNMP(newTable, 4);
				//long endTime = System.currentTimeMillis();

				//long timeElapsed1 = endTime - startTime;

				//System.out.println("Execution time in milliseconds for Index Creation : " + timeElapsed1);
			} catch (ParseException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	/*
        	NavigableMap<Date, ArrayList<String>> nm = new TreeMap<Date, ArrayList<String>>();
    		try {
				nm = Main.readNMap(newTable.tableName+".ind");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
        	
        }
        else if(newTable.tableName.equalsIgnoreCase("customer")) {
        	try {
        		//long startTime = System.currentTimeMillis();
				Main.BuildIndexHMP(newTable, 6);
				//long endTime = System.currentTimeMillis();

				//long timeElapsed1 = endTime - startTime;

				//System.out.println("Execution time in milliseconds for Index Creation : " + timeElapsed1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	//Not a Navigable Map
        	if(false) {
        	HashMap<String, ArrayList<Long>> h = new HashMap<String, ArrayList<Long>>();
        	try {
				h = readMap(newTable.tableName+".ind");
			} catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        	}
        	/*
        	for(String s: h.keySet()) {
        		System.out.println(h.get(s));
        	}
        	*/
        	
        }
        }
        }
        }
        
        
        /*
        RandomAccessFile fis = null;
        try {
			fis = new RandomAccessFile( ConfigureVariables.pathname+"CUSTOMER"+ConfigureVariables.FILEEXTENSION,"r");
			fis.seek(1476);
			System.out.println(fis.readLine());
		} catch (IOException e){
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        */
        
        
		 
    }
    

    public static void  unionStatement(Union union) throws ClassNotFoundException, IOException {
    	List<PlainSelect> plainSelectsList = union.getPlainSelects();
        for(PlainSelect ps : plainSelectsList) {
        	plainStatement(ps);
        	//System.out.println(ps);
        }
    	//System.out.println(plainSelectsList);
        //System.out.println("****Execute Plain Statement***");
		
	}
    
    /*
    public void plainSelect(SelectBody body, String basicPath) throws FileNotFoundException {
        PlainSelect plain = (PlainSelect) body;
        FromItem fromItem = plain.getFromItem();
        System.out.println(fromItem.toString());
        if (fromItem instanceof SubSelect || fromItem instanceof Select) {
            System.out.println("Able to detect Sub Select");
        }
        Table table = (Table) plain.getFromItem();

        String tableName = table.getName();
        String path = basicPath + tableName.toLowerCase() + ".csv";
        Scanner scanner = new Scanner(new File(path));
        scanner.useDelimiter(",");
        while (scanner.hasNext()) {
            System.out.print(scanner.next() + "|");
            System.out.print(scanner.nextLine());
        }
        scanner.close();
    }*/

 
    


	public static void plainStatement(PlainSelect plain) throws ClassNotFoundException, IOException {
		
		if(ConfigureVariables.index){
		HashMap<String,String> r = new HashMap<>();
		
		try {
			r = readMap("stats");
		} catch (ClassNotFoundException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		
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
	}
		
		//System.out.println("Plain Select Statement");
		
		ConfigureVariables.testselect = plain.getSelectItems();
		//SelectItem s = ConfigureVariables.testselect.get(0);
		for(SelectItem s : ConfigureVariables.testselect) {
			if(s instanceof SelectExpressionItem) {
				SelectExpressionItem se = (SelectExpressionItem)s;
				String ali = se.getAlias();
				if(ali==null) {
					ConfigureVariables.colalias.put(se.toString(),se.toString());
				}
				else {
					ConfigureVariables.colalias.put(se.toString(),ali);
				}
			}
		}
		
		/*
		for(String s : ConfigureVariables.colalias.keySet()) {
			System.out.println(s+":"+ConfigureVariables.colalias.get(s));
		}
		*/
		
		
		
		
		//System.out.println("TEST SELECT" + ConfigureVariables.testselect);
		
		
		
		
		/*
		
		
		
		
		
		List<SelectItem> selectItem = plain.getSelectItems();
		if(selectItem.get(0) instanceof AllColumns) {
			//System.out.println("Hello");
			ConfigureVariables.allcol = true;
		}
		else {
		for (SelectItem i :selectItem){
			String colalias = "";
			//SelectItem jHandler =  i;
			Expression selectExpressionItems = ((SelectExpressionItem) i).getExpression();
			
			ConfigureVariables.colExp.add(selectExpressionItems);
			
			String colName = selectExpressionItems.toString();
			//Column column = (Column)jHandler;
			//System.out.println("hello"+column);
			ConfigureVariables.colNameStrings.add(colName);
			//System.out.println(selectExpressionItems);
			if(((SelectExpressionItem) i).getAlias()!=null) {
				colalias = ((SelectExpressionItem) i).getAlias();
			//System.out.println(colalias);
			//Configuration.aliases.put(nameTable, selectExpressionItems);
			}
			else {
				colalias = selectExpressionItems.toString();
			}
			ConfigureVariables.aliases.put(colName,colalias);
			//i.accept(selectItemVisitor);
			//System.out.println(Configuration.aliases);
		}
		for(String key:ConfigureVariables.aliases.keySet())
		{
			//System.out.println(key+":"+ConfigureVariables.aliases.get(key));
		}
		}
		
		
		
		
		
		*/
		
		
		
		
		
		
		
		
		FromItem fromitem = plain.getFromItem();
		
		if(fromitem instanceof SubSelect) {
			SubSelectHandler.excecute(plain);
			if(plain.getWhere()!=null)
			{  Expression expression =plain.getWhere();
				ConfigureVariables.whereArrayList.add(expression);
			}

		}
		else {
			Table tbTable = (Table)plain.getFromItem();
			String nameString = tbTable.getName();
			String tbalias = tbTable.getAlias();
			//System.out.println("ALIS"+tbalias);
			if(tbalias!=null) {
				
				ConfigureVariables.tablealias.put(nameString,tbalias);
				ConfigureVariables.aliastotable.put(tbalias,nameString);
				ConfigureVariables.talias.add(tbalias);
			}
			else {
				ConfigureVariables.tablealias.put(nameString,nameString);
				ConfigureVariables.aliastotable.put(nameString,nameString);
			}
			/*
			for(String k : ConfigureVariables.tablealias.keySet()) {
				System.out.println(k +":" + ConfigureVariables.tablealias.get(k));
				
			}
			*/
			ConfigureVariables.tableStrings.add(nameString);
			//System.out.println(nameString);
			if(plain.getJoins()!=null)
			{
			List<Join> joins =  plain.getJoins();
			if (joins.contains("JOIN")) {
				ConfigureVariables.JOIN = 1;
			//System.out.println(joins.size());
				//Configuration.conditionsArrayList.add(true);
				
			for(Join i:joins)
			{
				//System.out.println(i.getRightItem());
				String rightString =i.getRightItem().toString();
				ConfigureVariables.tableStrings.add(rightString);
				if(i.getOnExpression()!=null) {
					Expression onExpression = i.getOnExpression();
					ConfigureVariables.OnList.add(onExpression);
					
				}
				
			}}else {
				ConfigureVariables.joinsget = true;
				
				
			for(Join i:joins) {
				//System.out.println("IIII"+i);
				//Table nameTable =
				Table tb1 = (Table)i.getRightItem();
				String table = tb1.getName();
				String tb = ((Table)tb1).getAlias();
				//System.out.println("TB 1:" +tb1);
				if(tb!=null) {
					ConfigureVariables.talias.add(tb);
					ConfigureVariables.tablealias.put(table,tb);
					ConfigureVariables.aliastotable.put(tb,table);
					}
					//System.out.println("TBBBBB"+tb);
					//ConfigureVariables.tablealias.put(nameString,tbalias);
				
				else {
					ConfigureVariables.tablealias.put(table,table);
					ConfigureVariables.aliastotable.put(table,table);
				}
				ConfigureVariables.tableStrings.add(table);
				//joinTables can be configured here if needed or else just use the tableStrings.size() to predict
			}
			/*
			System.out.println("TABLE ALIAS");
			for(String key : ConfigureVariables.tablealias.keySet()) {
				System.out.println(key +" :" + ConfigureVariables.tablealias.get(key));
			}
			System.out.println(ConfigureVariables.tableStrings);
			*/
			}
			
			}
			//else
			//System.out.println(joins);
			if(plain.getWhere()!=null) {
				Expression expression =plain.getWhere();
				//System.out.println(expression);
				// CREATE TABLE T(STRING NAME, AGE INT, YEAR INT);
				// SELECT T.NAME FROM T WHERE T.AGE>20;
				//InverseExpression i = new InverseExpression(expression);
				
				ConfigureVariables.whereArrayList.add(expression);
				
				//Dividing whereArrayList to nonJoinCondition and joinCondition 
				HashSet<Expression> hash = MainHandler.getSingleExpression(expression);
				//System.out.println("Single Expression"+hash);
				MainHandler.getJoinConditions(hash);
				//System.out.println("JOin Cond:"+ConfigureVariables.joinCondition);
				//System.out.println("NON JOIN CON:"+ConfigureVariables.nonJoinCondition);
				
				//System.out.println("NON:"+ConfigureVariables.nonJoinCondition);
				//System.out.println("JOIN:"+ConfigureVariables.joinCondition);
				//HashSet<Expression> hash= new HashSet();
				//hash.add(expression);
				
				
				//ConfigureVariables.tableCondition=mapConditions(hash);
				
				ConfigureVariables.tableCondition=mapConditions(ConfigureVariables.nonJoinCondition);
				/*
				for( String s : ConfigureVariables.tableCondition.keySet()) {
					System.out.println(s +":"+ ConfigureVariables.tableCondition.get(s));
				}
				*/
				//System.exit(0);
				/*
				for(String s: ConfigureVariables.tableCondition.keySet()) {
					System.out.println(s+":"+ConfigureVariables.tableCondition.get(s));
				}
				*/
				
				
			}
			if(plain.getLimit()!=null) {
				ConfigureVariables.limit = plain.getLimit().getRowCount();
				//System.out.println(ConfigureVariables.limit);
			}
			if(plain.getOrderByElements()!=null) {
				List<OrderByElement> orderby = plain.getOrderByElements();
				
				ConfigureVariables.orderby.addAll(orderby);
				//System.out.println(orderby);
				
			}
		//System.out.println("Hello");	
		}
		
		if(false){	if(ConfigureVariables.tableStrings.size()==6) {
			ConfigureVariables.istpch5=true;
		}
		if(ConfigureVariables.tableStrings.size()==3) {
			ConfigureVariables.istpch3=true;
		}
		if(ConfigureVariables.tableStrings.size()==1) {
			//System.out.println("1");
			if(ConfigureVariables.tableCondition.containsKey("LINEITEM")) {
			ArrayList<Expression> e = ConfigureVariables.tableCondition.get("LINEITEM");
			if(e.size()>1) {
				ConfigureVariables.istpch6=true;
				//System.out.println("set");
			}
			else {
				ConfigureVariables.istpch1=true;
			}
			}
		}
		if(ConfigureVariables.tableStrings.size()==2) {
			ConfigureVariables.istpch12=true;
		}
	}	
		//System.out.println("Plain Select Statement");
		if(ConfigureVariables.joinsget && ConfigureVariables.tableStrings.size()>1) {
			// CREATE TABLE Q(NAME STRING, AGE INT);
			//CREATE TABLE N(NAME STRING, AGE INT, VAL INT);
			//SELECT N.NAME, N.AGE*(1 - N.VAL) FROM N;
		    // CREATE TABLE T(NAME STRING, AGE INT, YEAR INT);
			// SELECT Q.NAME FROM Q,T WHERE Q.AGE=T.AGE;
			//SELECT Q.NAME, SUM(Q.AGE), T.NAME FROM Q,T WHERE Q.AGE=T.AGE GROUP BY Q.NAME ORDER BY Q.NAME;
			// SELECT Q.NAME, SUM(Q.AGE) as K, T.NAME FROM Q,T WHERE Q.AGE=T.AGE GROUP BY Q.NAME,T.NAME ORDER BY K;
			//SELECT X.NAME, SUM(X.AGE) as K FROM X,Y WHERE X.AGE=Y.AGE GROUP BY X.NAME;
			// SELECT T.NAME FROM T,Q WHERE T.AGE=Q.AGE AND T.AGE>1 AND T.YEAR>100;
			//SELECT T.NAME FROM T,Q WHERE T.AGE>1 AND T.YEAR>2000;
			//CREATE TABLE M(ID STRING, NAME STRING, LNAME STRING, AGE INT);
			//SELECT T.NAME FROM T,Q,M WHERE T.AGE=Q.AGE AND T.NAME = M.NAME AND T.AGE>10 ;
			//SELECT N.NAME , SUM(N.AGE*(1- N.VAL)) FROM Q,N WHERE Q.AGE = N.AGE GROUP BY N.NAME;
			/*for(String s: ConfigureVariables.tableObjects.keySet()) {
				System.out.println(s + ":"+ ConfigureVariables.tableObjects.get(s).tableName);
			}
			*/
			//SELECT E.NAME, SUM(E.AGE) FROM E,F WHERE E.AGE=F.AGE GROUP BY E.NAME;
			//For Testing Purpose
			//MyTable t1 = (MyTable)ConfigureVariables.tableObjects.get("T");
			//CREATE TABLE P(NAME STRING, AGE INT, VAL DECIMAL);
			//SELECT P.NAME, SUM(P.AGE*(1-P.VAL)) FROM P,Z WHERE P.AGE=Z.AGE GROUP BY P.NAME;
			//MyTable t2 = (MyTable)ConfigureVariables.tableObjects.get("Q");
			//OnePassHashJoin.computeJoin(t1, t2);
			
			//SELECT P.NAME, SUM(P.AGE*(1-P.VAL)) FROM P,Z WHERE P.AGE=Z.AGE GROUP BY P.NAME;
			
			//CREATE TABLE P(NAME STRING, AGE INT, VAL DECIMAL);
			//CREATE TABLE Z(NAME STRING, AGE INT, VAL DECIMAL);
			
			//
			
			
			//CREATE TABLE Q(NAME STRING, AGE INT);
			//CREATE TABLE T(NAME STRING, AGE INT, YEAR INT);
			//SELECT Q.NAME , SUM(Q.AGE) FROM Q,T WHERE Q.AGE = T.AGE GROUP BY Q.AGE LIMIT 1;
			
			
			//System.out.println(ConfigureVariables.tableStrings.size());
			//ConfigureVariables.inmem = false;
			//Evaluate Join with Selection PushDown
			
			/*
			for(String s : ConfigureVariables.tableCondition.keySet()) {
				System.out.println(s +":"+ ConfigureVariables.tableCondition.get(s));
			}
			
			*/
			
			
			
			if(ConfigureVariables.inmem) {
				MyTable res = null;
				MyTable t1 = (MyTable)ConfigureVariables.tableObjects.get(ConfigureVariables.tableStrings.get(0));
				//MyTable t2 = (MyTable)ConfigureVariables.tableObjects.get(1);
				
				for(int i=1;i<ConfigureVariables.tableStrings.size();i++) {
					
					//MyTable t = OnePassHashJoin.computeJoin(t1, t2);
					//t1 = t;
					//t2 = (MyTable)ConfigureVariables.tableObjects.get(i+2);
					
					MyTable two = (MyTable)ConfigureVariables.tableObjects.get(ConfigureVariables.tableStrings.get(i));
					 res= OnePassHashJoin.computeJoin(t1,two);
					t1=res;
					ConfigureVariables.firstjoin=true;
					
			}	
				//System.out.println("SIZE::"+res.tuples.size());
				//writeresult(res.tuples);
				ConfigureVariables.beforejoin=false;
				ConfigureVariables.afterjoincol = res.columnNames;
				/*
				for(int i=0;i<res.tuples.size();i++) {
					System.out.println(res.tuples.get(i));
				}
				*/
				
				//System.out.println("Array"+res.tableColDef);
				ArrayList<String> listlast = new ArrayList<>();
			
				
					if(plain.getGroupByColumnReferences()!=null) {
					List groupbyElementsList = null;
					if(plain.getGroupByColumnReferences()!=null)
						groupbyElementsList = plain.getGroupByColumnReferences();
					if(groupbyElementsList!=null) {
					ConfigureVariables.groupbylist.addAll(groupbyElementsList);
					}	
					
					
					if(groupbyElementsList!=null) {
						ConfigureVariables.groupByPresent=true;
						try {
							GroupByModified.getGroupByJO(res, groupbyElementsList);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
			
					
					for(String s:ConfigureVariables.computedGroupByo.keySet()) {
						ArrayList<Object> temp = ConfigureVariables.computedGroupByo.get(s);
						String tempo="";
						
						for( int i =0;i<temp.size();i++) {
							if(i==(temp.size()-1))
							{
								tempo+=temp.get(i);
							}
							else{
								tempo += temp.get(i)+"|";
							}
						
						
						}	
						listlast.add(tempo);
						
					}
					//System.out.println("LIST::::"+listlast);
					}
					
					
					if(plain.getOrderByElements()!=null) {
						List<OrderByElement> orderby = plain.getOrderByElements();
						//System.out.println("orderby::"+orderby);
						
						//ConfigureVariables.orderby.addAll(orderby);
						//System.out.println("Configorderby::"+ConfigureVariables.orderby);
						for(OrderByElement o:orderby) {
							if(o.toString().contains(" DESC")){
								ConfigureVariables.descending = true;
								//System.out.println("true");
								break;
							}
						}
						//System.out.println(orderby);
						try {
							listlast = Orderbymem.orderby(listlast,ConfigureVariables.groupbycd, ConfigureVariables.orderby);
						} catch (IOException e2) {
							// TODO Auto-generated catch block
							e2.printStackTrace();
						}
						Projection.project(listlast);
				
				}
					if(!ConfigureVariables.groupByPresent && plain.getOrderByElements()==null) {
						//When orderby is null
						Projection.projectdataobject(res);
						//for(int i =0; i<ConfigureVariables.limit;i++) {
						//	System.out.println(listlast.get(i));
						//}
					}	
					
					if(ConfigureVariables.groupByPresent && plain.getOrderByElements()==null) {
						Projection.project(listlast);
					}
			//System.out.println(res.readtuple());
			
			//TableScan.NestedScan(ConfigureVariables.tableStrings);
		
		}
			if(ConfigureVariables.ondisk){
				
				MyTable res = null;
				MyTable t1 = (MyTable)ConfigureVariables.tableObjects.get(ConfigureVariables.tableStrings.get(0));
				//MyTable t2 = (MyTable)ConfigureVariables.tableObjects.get(1);
				
				for(int i=1;i<ConfigureVariables.tableStrings.size();i++) {
					
					//MyTable t = OnePassHashJoin.computeJoin(t1, t2);
					//t1 = t;
					//t2 = (MyTable)ConfigureVariables.tableObjects.get(i+2);
					
					MyTable two = (MyTable)ConfigureVariables.tableObjects.get(ConfigureVariables.tableStrings.get(i));
					 res= HashJoin.computeJoin(t1,two);
					t1=res;
					
			}
				//System.out.println("RES::::"+res.columnNames);
				ConfigureVariables.beforejoin=false;
				ConfigureVariables.afterjoincol = res.columnNames;
				
				
				//System.out.println("Array"+res.tableColDef);
				ArrayList<String> listlast = new ArrayList<>();
			
				
					if(plain.getGroupByColumnReferences()!=null) {
					List groupbyElementsList = null;
					if(plain.getGroupByColumnReferences()!=null)
						groupbyElementsList = plain.getGroupByColumnReferences();
					if(groupbyElementsList!=null) {
					ConfigureVariables.groupbylist.addAll(groupbyElementsList);
					}	
					
					
					if(groupbyElementsList!=null) {
						ConfigureVariables.groupByPresent=true;
						
							
								try {
									ExternalSort e = new ExternalSort();
									e.orderbyGroup(res, groupbyElementsList);
									//System.out.println("Final file data");
									//Projection.projectfile("finalfile");
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							
							MyTable tnew = new MyTable("finalfile",res.tableColDef,false);
							tnew.columnNames = res.columnNames;
							tnew.columnDatatype= res.columnDatatype;
							//System.out.println("Final FIle Data:");
							//Projection.projectdataobject(tnew);
							//MyTable t = ConfigureVariables.tableObjects.get("finalfile");
							try {
								if(groupbyElementsList!=null) {
								Groupbyondisk.groupby(tnew, groupbyElementsList);
								}
								//System.out.println("GroupedBYCD");
								//System.out.println(ConfigureVariables.groupbycd);
							} catch (SQLException | IOException e) {
								 //TODO Auto-generated catch block
								e.printStackTrace();
							}
							
							//Getting groupedfile for groupby
							
						
					}
					
					
				
			}
					
					if(plain.getOrderByElements()!=null) {
						if(ConfigureVariables.groupByPresent) {
					MyTable atb = new MyTable("groupedfile", ConfigureVariables.groupbycd,false);
						ExternalSort e = new ExternalSort();
						try {
							e.orderby(atb, ConfigureVariables.orderby);
						} catch (IOException e1) {
							 //TODO Auto-generated catch block
							e1.printStackTrace();
						}
						}
						else {
							ExternalSort e = new ExternalSort();
							try {
								e.orderby(res, ConfigureVariables.orderby);
							} catch (IOException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						}
						
						
						try {
							Projection.projectfile("finalfile");
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						//Printout final file
					}
					
					else {
						if(ConfigureVariables.groupByPresent) {
							//printout grouped file
							try {
								Projection.projectfile("groupedfile");
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						else {
						Projection.projectdataobject(res);
						}
						//Printout res file
					}
		
			}
			
			}
		else {
		
		
		MyTable t1 = (MyTable)ConfigureVariables.tableObjects.get(ConfigureVariables.tableStrings.get(0));
		ArrayList<String> cn = new ArrayList<String>();
	     ArrayList<ColDataType> cd = new ArrayList<ColDataType>();
	     
	        for(int i=0;i<t1.columnNames.size();i++) {
	        	cn.add(t1.columnNames.get(i));
	        	cd.add(t1.columnDatatype.get(i));
	        }
	    t1.columnNames=cn;
	    t1.columnDatatype = cd;
	    
	    if(false){
	    if(t1.tableName.equals("LINEITEM")) {
	    	ArrayList<Expression> e = ConfigureVariables.tableCondition.get(t1.tableName);
	    	if(e.size()==1) {
	    		if(e.get(0).toString().contains("SHIPDATE")) {
	    			ConfigureVariables.istpch1=true;
	    			//System.out.println("YES");
	    		}
	    	}
	    }
		}
		//System.out.println("Here"+t1.tableName);
		//MyTable f = SingleTableWhere.FilterWhere(t1);
		//System.out.println("Starting!!!!!!!");
		//Projection.projectdataobject(f);
		//System.out.println("End!!!!!!!!");
		
		ArrayList<String> listlast = new ArrayList<>();
		
		
		if(plain.getGroupByColumnReferences()!=null) {
		List groupbyElementsList = null;
		if(plain.getGroupByColumnReferences()!=null)
			groupbyElementsList = plain.getGroupByColumnReferences();
		if(groupbyElementsList!=null) {
		ConfigureVariables.groupbylist.addAll(groupbyElementsList);
		}	
		
		
		if(groupbyElementsList!=null) {
			ConfigureVariables.groupByPresent=true;
			try {
				GroupByModified.getGroupByWO(t1, groupbyElementsList);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		
		for(String s:ConfigureVariables.computedGroupByo.keySet()) {
			ArrayList<Object> temp = ConfigureVariables.computedGroupByo.get(s);
			String tempo="";
			
			for( int i =0;i<temp.size();i++) {
				if(i==(temp.size()-1))
				{
					tempo+=temp.get(i);
				}
				else{
					tempo += temp.get(i)+"|";
				}
			
			
			}	
			listlast.add(tempo);
			
		}
		//System.out.println("LIST::::"+listlast);
		}
		
		
		if(plain.getOrderByElements()!=null) {
			List<OrderByElement> orderby = plain.getOrderByElements();
			//System.out.println("orderby::"+orderby);
			
			//ConfigureVariables.orderby.addAll(orderby);
			//System.out.println("Configorderby::"+ConfigureVariables.orderby);
			for(OrderByElement o:orderby) {
				if(o.toString().contains(" DESC")){
					ConfigureVariables.descending = true;
					//System.out.println("true");
					break;
				}
			}
			//System.out.println(orderby);
			try {
				listlast = Orderbymem.orderby(listlast,ConfigureVariables.groupbycd, ConfigureVariables.orderby);
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			Projection.project(listlast);
	
	}
		if(!ConfigureVariables.groupByPresent && plain.getOrderByElements()==null) {
			//When both groupby and orderby is null
			
			
			//Projection.projectdataobjectsingle(f);
			//Projection.projectdataobjectsingleagg(f);
			//Projection.proj(f);
			
			TableScan.SimpleScan(t1,ConfigureVariables.tablefile, ConfigureVariables.tableStrings);
			
			
			//for(int i =0; i<ConfigureVariables.limit;i++) {
			//	System.out.println(listlast.get(i));
			//}
		}	
		
		if(ConfigureVariables.groupByPresent && plain.getOrderByElements()==null) {
			Projection.project(listlast);
		}
		
		
		
		
		
		//TableScan.SimpleScan(ConfigureVariables.tablefile, ConfigureVariables.tableStrings);
		}
		
	}
	

	
	//MyTable t = ConfigureVariables.tableObjects.get("Q");
	
	//MyTable tab =  new MyTable("L",ConfigureVariables.groupbycd);
	
	
	
	//CREATE TABLE Q(Rate STRING,Bike INT,Cat DATE);
	// SELECT Rate,SUM(Bike) AS K FROM Q GROUP BY Rate ORDER BY K;
	
	
	// SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1999-03-13') AND LINEITEM.RETURNFLAG = 'A' AND LINEITEM.LINESTATUS ='F';
	
	
	
	
	
	
	
	
	
	
	public static void getJoinConditions(HashSet<Expression> breakExpressionSet) {
		
		for(Expression expression : breakExpressionSet ) {
			boolean flag = false;
			if(!(expression instanceof EqualsTo)) {
				ConfigureVariables.nonJoinCondition.add(expression);
			}
			else {
				Expression left = ((EqualsTo) expression).getLeftExpression();
				Expression right = ((EqualsTo) expression).getRightExpression();
				for(String s : ConfigureVariables.tableStrings) {
					if(right.toString().contains(s+".")) {
						ConfigureVariables.joinCondition.add(expression);
						flag=true;
						break;
					}
					
				}
				if(!flag) {
					ConfigureVariables.nonJoinCondition.add(expression);
				}
			}
		}
	}
	public static HashSet<Expression> getSingleExpression(Expression expression){
		HashSet<Expression> hashSet = new HashSet<Expression>();
		Expression leftVal = null;
		Expression rightVal = null;

		if (expression instanceof AndExpression) {
		AndExpression and = (AndExpression) expression;
		leftVal = ((Expression) and.getLeftExpression());
		rightVal = ((Expression) and.getRightExpression());

		if (leftVal instanceof AndExpression) {
		HashSet<Expression> array = getSingleExpression(leftVal);
		for (Expression s : array) {
		hashSet.add(s);
		}
		hashSet.add(rightVal);
		} else {
		hashSet.add(leftVal);
		hashSet.add(rightVal);
		}

		} 
		return hashSet;
		}
	
	public static HashMap<String, ArrayList<Expression>>  mapConditions(HashSet<Expression> hash){
		HashMap<String,ArrayList<Expression>> map = new HashMap();
		Expression left =null;
		Expression right = null;
		Expression leftexp=null;
		Expression rightexp=null;
		for(Expression e : hash) {
			ArrayList<Expression> l = new ArrayList();
			l.add(e);
			left =  (Expression)((BinaryExpression) e).getLeftExpression();
			right = (Expression)((BinaryExpression) e).getRightExpression();
			if(left instanceof Column) {
				//System.out.println("Column Name:"+(String)(((Column) left).getColumnName()));
				//System.out.println("TableName:"+(String)(((Column) left).getTable().getName()));
				String tbl = (String)(((Column) left).getTable().getName());
				if(map.containsKey(tbl)) {
					map.get(tbl).add(e);
				}
				else {
					map.put(tbl,l);
				}
			}
			else if(e instanceof OrExpression){
				leftexp =  (Expression)((BinaryExpression) e).getLeftExpression();
				rightexp = (Expression)((BinaryExpression) e).getRightExpression();
				left =  (Expression)((BinaryExpression) leftexp).getLeftExpression();
				right = (Expression)((BinaryExpression) leftexp).getRightExpression();
				String tbl = (String)(((Column) left).getTable().getName());
				if(map.containsKey(tbl)) {
					map.get(tbl).add(e);
				}
				else {
					map.put(tbl,l);
				}
				
				//System.out.println("LEFT:"+left+":"+right);
			}
			//System.out.println(left);
			//System.out.println(right);
		}
		
		
		return map;
		
	}
	//HashMap<String, ArrayList<Expression>>
	
	
public static void writemap(HashMap hmap,String f) throws IOException {
		
		
	    FileOutputStream fos;
		try {
			fos = new FileOutputStream(ConfigureVariables.temppath+f);
		
	    ObjectOutputStream oos = new ObjectOutputStream(fos);
	        
	        oos.writeObject(hmap);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

public static void writeresult(ArrayList<String> jrow)  {
	
	FileWriter fw = null;
	BufferedWriter bw =null;
	File f = new File(ConfigureVariables.pathname+"RESULT"+ConfigureVariables.FILEEXTENSION);
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
	for(int i =0; i<jrow.size();i++) {
	bw.write(jrow.get(i)+"\n");
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
		ois  = new ObjectInputStream(fis);
		h = (HashMap) ois.readObject();
		return h;
		
	}
	return null;
	
	}

}
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		////////////////////////////////////////////////////////////////////////////////
		/*
		FromItemChecker fic = new FromItemChecker();
		fic.check(fromtable);
		Table table = (Table) plain.getFromItem();
		System.out.println("From Items:" + fromtable);
		System.out.println("Select:"+ selectItem);
		
		for (Map.Entry<String, HashMap<String, ColumnInfo>> entry : dbMap.entrySet())
		{
			String s = entry.getKey();
			HashMap<String, ColumnInfo> c = entry.getValue();
			for (Map.Entry<String, ColumnInfo> entry1 : c.entrySet())
			{
				String s1 = entry1.getKey();
				ColumnInfo c1 = entry1.getValue();
	
				System.out.println(s+":"+s1 +" : "+ c1.colNo +":"+c1.colDataType );
			}
			//System.out.println(s +" : "+ c );
		}
		System.out.println("****************");
		if(plain.getWhere()!=null) {
			System.out.println(plain.getWhere());
			Expression exp = plain.getWhere();
			
			//Tuple.getTuple(line, createTableMap, tableName)
			//Evaluator eval = new Evaluator(tuple,dbMap);
			
			
		}
		*/
		
	

