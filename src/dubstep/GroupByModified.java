package dubstep;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
////@Author - Apoorva Biseria and Thejasweni Prakash Mysore
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByModified {
	
	public static void getGroupBy(MyTable table, List groupbyElementsList) throws SQLException {
		//System.out.println("INSIDE FUNCTION");
		//System.out.println("Hello:"+groupbyElementsList);
		ArrayList<String> colNames = new ArrayList<>();
		String row;
		ArrayList<String> ColumnNames =table.columnNames;
		ArrayList<ColDataType> datatypes =table.columnDatatype;
		ArrayList<ColumnDefinition> columns = table.tableColDef;
		ArrayList<Integer> indexes = new ArrayList<>();
		ArrayList<Integer> indexes2 = new ArrayList<>();
		//ArrayList<Integer> sumindex= new ArrayList<>();
		ArrayList<String> rows = table.tuples;
		HashMap<String,ArrayList<Integer>> sumindexes  =  new HashMap<>();
 		
		//ArrayList<String> selectindexes =new ArrayList<>();
		int kt=0;
		ArrayList<ColumnDefinition> columns2 = new ArrayList<ColumnDefinition>();
		for(int i =0;i<ColumnNames.size();i++)
		{
			
			
			ColumnDefinition col = new ColumnDefinition();
			col.setColumnName(ColumnNames.get(i));
			col.setColDataType(datatypes.get(i));
			columns2.add(col);
		}
		//System.out.println("COLUMNS222"+columns2);
		//ArrayList<MyTable> tables = new ArrayList();
		//HashMap<Integer,MyTable> tableinfo =  new HashMap();
		/*SelectItem aggo=null;
		int i =0;
		int fi = 0;
		for(SelectItem si : ConfigureVariables.testselect) {
			//System.out.println("Select Item:"+si);
			if(si.toString().contains("SUM")) {
				aggo = si;
				fi =i;
				//System.out.println("FI:::"+fi);
				//System.out.println("AGGO::"+aggo);
				
			}
			i++;
				}*/
		int g = 0;
		
	
		
			
		
		/*for ( i=0;i<columns.size();i++) {
			 String colName = columns.get(i).getColumnName().toLowerCase();
			 ColumnNames.add(colName);
	         ColDataType colDataType = columns.get(i).getColDataType();
	         datatypes.add(colDataType);
		}*/
		
		//indexes of groupby columns
		for(Object o : groupbyElementsList) {
					int l = 0;
				for(String s : ColumnNames) {
						if(o.toString().toLowerCase().equals(s.toLowerCase())) {
							indexes.add(l);
						}
						l++;
					}
				
		}	
		/*
		for(SelectItem s:ConfigureVariables.testselect) {
			System.out.println("Select");
			System.out.println(s.toString());
		}
		*/
		
		for(SelectItem s:ConfigureVariables.testselect)
		{	
			int l=0;
			for(String s1 : ColumnNames) {
				
					 if(s.toString().toLowerCase().equals(s1.toLowerCase())) {
						 {	indexes2.add(l);
					break;}
						
				}
					 else {
						 if(s.toString().toLowerCase().contains("sum")) {
						 if(s.toString().toLowerCase().contains(s1.toString().toLowerCase())) {
							 indexes2.add(l);
							 break;
						 }}
					 }
				l++;
			}
		}
		//System.out.println("Indexes2:"+indexes2);
		//ArrayList<ColumnDefinition> newcd = new ArrayList<ColumnDefinition>();
		for(Integer h:indexes2)
		{
		ColumnDefinition col = new ColumnDefinition();
		
		col.setColumnName(ColumnNames.get(h));
		col.setColDataType(datatypes.get(h));
		//System.out.println("Heree:"+h +":"+ColumnNames.get(h));
		 ConfigureVariables.groupbycd.add(col);
		// System.out.println( "Grouped by" + ConfigureVariables.groupbycd.get(g));
		// g++;
		}
		for(ColumnDefinition c:ConfigureVariables.groupbycd) {
			for(SelectItem s:ConfigureVariables.testselect) {
				if(s.toString().toLowerCase().contains(c.getColumnName().toLowerCase()))
				{	
					String alias =((SelectExpressionItem) s).getAlias();;
					if(alias!=null) {
						{
							//System.out.println(alias+ "=alias");
						c.setColumnName(alias);
						}
					}
					
				}
			}
			
		}
		//System.out.println(ConfigureVariables.groupbycd+"LAALALALLAL");
		
		
		//ArrayList<String > med = new ArrayList<>();
		
		int ind=0;
		//int size = rows.size() -1;
		int size = rows.size();
		while(ind < size) {
			row = rows.get(ind);
			ind = ind +1;
		//while((row = table.readtuple())!=null) {
			String tempRow[] = row.split("\\|");
			String finalKey="";
			for( int i=0; i< indexes.size(); i++) {
				String tempKey = tempRow[indexes.get(i)];
				finalKey += tempKey + "|";
				
				
			}
			
			if(!sumindexes.containsKey(finalKey))
			{
				ArrayList<Integer> s= new ArrayList();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					s.add(0);
				}
				sumindexes.put(finalKey,s);
			}
			//System.out.println("Lastkey:"+"value"+lastKey);
			//System.out.println("Finalkey:"+"value"+finalKey);
			//if(finalKey.equals(lastKey)) {
				
			//System.out.println("final");
			int z=0;
			if(!ConfigureVariables.computedGroupBy.containsKey(finalKey))
			{
				ArrayList<String > med = new ArrayList<>();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					med.add(null);
				}
				ConfigureVariables.computedGroupBy.put(finalKey,med);
			}
			
			for(SelectItem si:ConfigureVariables.testselect) {
			PrimitiveValue[] t = Tuple.getTupleVal(row, columns2);
			
			//if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
				//int k = ConfigureVariables.computedGroupBy.get(finalKey);
			
				//aggregate for already containing key
			if(!(si.toString().contains("SUM")||si.toString().contains("AVG")||si.toString().contains("COUNT")||si.toString().contains("MAX")||si.toString().contains("MIN"))) {
				if(si instanceof SelectExpressionItem) {
					Evaluator eval = new Evaluator(t);
					try {
						PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
						//System.out.println(result.toRawString());
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z,result.toRawString());
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z,result.toRawString());
							
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
						
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
			}}
			else if(si.toString().contains("SUM")) {
				
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println("Expression:"+exp1);
					
					Evaluator eval = new Evaluator(t);
					
						PrimitiveValue result = eval.eval(exp1);
						//System.out.println(result.toRawString());
						if(result instanceof DoubleValue) {
							//ConfigureVariables.countcheck = ConfigureVariables.countcheck + 1;
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								if(k.get(z)==null) {
									k.set(z, 0);
								}
								Double sum = Double.parseDouble(k.get(z).toString()) + ((DoubleValue) result).getValue();
								k.set(z, sum.toString());
								ConfigureVariables.computedGroupBy.put(finalKey,k);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								k.set(z, result.toString());
								 ConfigureVariables.computedGroupBy.put(finalKey, k);
								 z++;
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
								
								if(k1.get(z)==null) {
									k1.set(z, 0);
								}
								Long sum = Long.parseLong(k1.get(z).toString()) + ((LongValue) result).getValue();
								k1.set(z, sum.toString());
								ConfigureVariables.computedGroupBy.put(finalKey, k1);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								k.set(z, result.toRawString());
								 ConfigureVariables.computedGroupBy.put(finalKey,k);
								 z++;
							}
						}
					}
			else if(si.toString().contains("AVG")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(t);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println("RESULT:::"+result.toRawString());
					
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							//System.out.println("yes");
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							//System.out.println("KKKKK"+k);
							if(k.get(z)==null) {
								k.set(z, 0);
							}
							Double t0 = Double.parseDouble(k.get(z).toString())*sumindexes.get(finalKey).get(z);
							Double sum = t0 + ((DoubleValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							k.set(z, avg.toString());
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg.toString());
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, 0);
							}
							
							Long t0 = Long.parseLong(k1.get(z).toString())*sumindexes.get(finalKey).get(z);
							Long sum = t0 + ((LongValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							//med.set(z, sum.toString());
							k1.set(z, avg.toString());
							ConfigureVariables.computedGroupBy.put(finalKey, k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg.toString());
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
				}
			else if(si.toString().contains("COUNT")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				//System.out.println(exp+"helllo");
				Function funct = (Function)exp;
				//System.out.println(funct+"helllo11111111");
				//Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				//Evaluator eval = new Evaluator(t,ConfigureVariables.dbMap);
				
				//	PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, 0);
							}
							int sum =0;
							ArrayList o= sumindexes.get(finalKey);
							for(int i= 0;i<o.size();i++) {
								if((int) o.get(i)!=0) {
								sum =  (int) o.get(i);
								break;}
							}
							//Long add = (long) 1;
							//Long sum2 = Long.parseLong(;
							
							k.set(z, Integer.toString(sum));
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							System.out.println("here");
							k.set(z, "1");
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
					
				}
			else if(si.toString().contains("MAX")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(t);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble(k.get(z).toString())>((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble(k.get(z).toString());
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum.toString());
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result.toString());
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong(k1.get(z).toString())>((LongValue) result).getValue())
							{
							sum =Long.parseLong(k1.get(z).toString());
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum.toString());
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result.toRawString());
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
					
					
				//inside si
			}
				else if(si.toString().contains("MIN")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(t);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble(k.get(z).toString())<((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble(k.get(z).toString());
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum.toString());
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result.toString());
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong(k1.get(z).toString())<((LongValue) result).getValue())
							{
							sum =Long.parseLong(k1.get(z).toString());
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum.toString());
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result.toRawString());
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
			//lastKey = finalKey;
			}
			//for(String s:sumindexes.keySet()) {
			//	System.out.println("SUM INDEX:"+sumindexes.get(s));
			//}
		
		
		}}}
	
	
	public static void getGroupByW(MyTable table, List groupbyElementsList) throws SQLException {
		//System.out.println("INSIDE FUNCTION");
		//System.out.println("Hello:"+groupbyElementsList);
		ArrayList<String> colNames = new ArrayList<>();
		String row;
		ArrayList<String> ColumnNames =table.columnNames;
		ArrayList<ColDataType> datatypes =table.columnDatatype;
		ArrayList<ColumnDefinition> columns = table.tableColDef;
		ArrayList<Integer> indexes = new ArrayList<>();
		ArrayList<Integer> indexes2 = new ArrayList<>();
		//ArrayList<Integer> sumindex= new ArrayList<>();
		HashMap<String,ArrayList<Integer>> sumindexes  =  new HashMap<>();
 		
		//ArrayList<String> selectindexes =new ArrayList<>();
		int kt=0;
		ArrayList<ColumnDefinition> columns2 = new ArrayList<ColumnDefinition>();
		for(int i =0;i<ColumnNames.size();i++)
		{
			
			
			ColumnDefinition col = new ColumnDefinition();
			col.setColumnName(ColumnNames.get(i));
			col.setColDataType(datatypes.get(i));
			columns2.add(col);
		}
		//System.out.println("COLUMNS222"+columns2);
		//ArrayList<MyTable> tables = new ArrayList();
		//HashMap<Integer,MyTable> tableinfo =  new HashMap();
		/*SelectItem aggo=null;
		int i =0;
		int fi = 0;
		for(SelectItem si : ConfigureVariables.testselect) {
			//System.out.println("Select Item:"+si);
			if(si.toString().contains("SUM")) {
				aggo = si;
				fi =i;
				//System.out.println("FI:::"+fi);
				//System.out.println("AGGO::"+aggo);
				
			}
			i++;
				}*/
		int g = 0;
		
	
		
			
		
		/*for ( i=0;i<columns.size();i++) {
			 String colName = columns.get(i).getColumnName().toLowerCase();
			 ColumnNames.add(colName);
	         ColDataType colDataType = columns.get(i).getColDataType();
	         datatypes.add(colDataType);
		}*/
		
		//indexes of groupby columns
		for(Object o : groupbyElementsList) {
					int l = 0;
				for(String s : ColumnNames) {
						if(o.toString().toLowerCase().equals(s.toLowerCase())) {
							indexes.add(l);
						}
						l++;
					}
				
		}	
		/*
		for(SelectItem s:ConfigureVariables.testselect) {
			System.out.println("Select");
			System.out.println(s.toString());
		}
		*/
		
		for(SelectItem s:ConfigureVariables.testselect)
		{	
			int l=0;
			for(String s1 : ColumnNames) {
				
					 if(s.toString().toLowerCase().equals(s1.toLowerCase())) {
						 {	indexes2.add(l);
					break;}
						
				}
					 else {
						 if(s.toString().toLowerCase().contains("sum")) {
						 if(s.toString().toLowerCase().contains(s1.toString().toLowerCase())) {
							 indexes2.add(l);
							 break;
						 }}
					 }
				l++;
			}
		}
		//System.out.println("Indexes2:"+indexes2);
		//ArrayList<ColumnDefinition> newcd = new ArrayList<ColumnDefinition>();
		for(Integer h:indexes2)
		{
		ColumnDefinition col = new ColumnDefinition();
		
		col.setColumnName(ColumnNames.get(h));
		col.setColDataType(datatypes.get(h));
		//System.out.println("Heree:"+h +":"+ColumnNames.get(h));
		 ConfigureVariables.groupbycd.add(col);
		// System.out.println( "Grouped by" + ConfigureVariables.groupbycd.get(g));
		// g++;
		}
		for(ColumnDefinition c:ConfigureVariables.groupbycd) {
			for(SelectItem s:ConfigureVariables.testselect) {
				if(s.toString().toLowerCase().contains(c.getColumnName().toLowerCase()))
				{	
					String alias =((SelectExpressionItem) s).getAlias();;
					if(alias!=null) {
						{
							//System.out.println(alias+ "=alias");
						c.setColumnName(alias);
						}
					}
					
				}
			}
			
		}
		//System.out.println(ConfigureVariables.groupbycd+"LAALALALLAL");
		
		
		//ArrayList<String > med = new ArrayList<>();
		ArrayList<String> sit = new ArrayList<String>();
		for(SelectItem si : ConfigureVariables.testselect) {
			sit.add(si.toString());
		}
		
		
		while((row = table.readtuple())!=null) {
			PrimitiveValue tempRow[] ;
			tempRow = checkSelection(row, table.tableColDef,table.tableName);
			
			if(tempRow!=null) {
			//String tempRow[] = row.split("\\|");
			String finalKey="";
			for( int i=0; i< indexes.size(); i++) {
				String tempKey = tempRow[indexes.get(i)].toRawString();
				finalKey += tempKey + "|";
				
				
			}
			
			if(!sumindexes.containsKey(finalKey))
			{
				ArrayList<Integer> s= new ArrayList();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					s.add(0);
				}
				sumindexes.put(finalKey,s);
			}
			//System.out.println("Lastkey:"+"value"+lastKey);
			//System.out.println("Finalkey:"+"value"+finalKey);
			//if(finalKey.equals(lastKey)) {
				
			//System.out.println("final");
			int z=0;
			if(!ConfigureVariables.computedGroupBy.containsKey(finalKey))
			{
				ArrayList<String > med = new ArrayList<>();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					med.add(null);
				}
				ConfigureVariables.computedGroupBy.put(finalKey,med);
			}
			int sitem=0;
			for(SelectItem si:ConfigureVariables.testselect) {
			//PrimitiveValue[] t = Tuple.getTupleVal(row, columns2);
			
			//if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
				//int k = ConfigureVariables.computedGroupBy.get(finalKey);
			
				//aggregate for already containing key
				String s = sit.get(sitem);
			if(!(s.contains("SUM")||s.contains("AVG")||s.contains("COUNT")||s.contains("MAX")||s.contains("MIN"))) {
				//!(si.toString().contains("SUM")||si.toString().contains("AVG")||si.toString().contains("COUNT")||si.toString().contains("MAX")||si.toString().contains("MIN"))
				if(si instanceof SelectExpressionItem) {
					Evaluator eval = new Evaluator(tempRow);
					try {
						PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
						//System.out.println(result.toRawString());
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z,result.toRawString());
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z,result.toRawString());
							
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
						
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
			}}
			else if(s.contains("SUM")) {
				
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println("Expression:"+exp1);
					Evaluator eval = new Evaluator(tempRow);
					
						PrimitiveValue result = eval.eval(exp1);
						//System.out.println(result.toRawString());
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								ArrayList<String> k = ConfigureVariables.computedGroupBy.get(finalKey);
								if(k.get(z)==null) {
									k.set(z, "0");
								}
								Double sum = Double.parseDouble(k.get(z)) + ((DoubleValue) result).getValue();
								k.set(z, sum.toString());
								ConfigureVariables.computedGroupBy.put(finalKey,k);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupBy.put(finalKey, k);
								 z++;
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
								
								if(k1.get(z)==null) {
									k1.set(z, 0);
								}
								Long sum = Long.parseLong((String) k1.get(z)) + ((LongValue) result).getValue();
								k1.set(z, sum);
								ConfigureVariables.computedGroupBy.put(finalKey, k1);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupBy.put(finalKey,k);
								 z++;
							}
						}
					}
			else if(s.contains("AVG")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println("RESULT:::"+result.toRawString());
					
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							//System.out.println("yes");
							ArrayList<String> k = ConfigureVariables.computedGroupBy.get(finalKey);
							//System.out.println("KKKKK"+k);
							if(k.get(z)==null) {
								k.set(z, "0");
							}
							Double t0 = Double.parseDouble(k.get(z))*sumindexes.get(finalKey).get(z);
							Double sum = t0 + ((DoubleValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							k.set(z, avg.toString());
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg);
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, "0");
							}
							
							Long t0 = Long.parseLong((String) k1.get(z))*sumindexes.get(finalKey).get(z);
							Long sum = t0 + ((LongValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							//med.set(z, sum.toString());
							k1.set(z, avg);
							ConfigureVariables.computedGroupBy.put(finalKey, k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg);
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
				}
			else if(s.contains("COUNT")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				//System.out.println(exp+"helllo");
				Function funct = (Function)exp;
				//System.out.println(funct+"helllo11111111");
				//Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				//Evaluator eval = new Evaluator(t,ConfigureVariables.dbMap);
				
				//	PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, 0);
							}
							int sum =0;
							ArrayList o= sumindexes.get(finalKey);
							for(int i= 0;i<o.size();i++) {
								if((int) o.get(i)!=0) {
								sum =  (int) o.get(i);
								break;}
							}
							//Long add = (long) 1;
							//Long sum2 = Long.parseLong(;
							
							k.set(z, Integer.toString(sum));
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							System.out.println("here");
							k.set(z, "1");
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
					
				}
			else if(s.contains("MAX")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble((String) k.get(z))>((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble((String) k.get(z));
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong((String) k1.get(z))>((LongValue) result).getValue())
							{
							sum =Long.parseLong((String) k1.get(z));
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
					
					
				//inside si
			}
				else if(s.contains("MIN")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble((String) k.get(z))<((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble((String) k.get(z));
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong((String) k1.get(z))<((LongValue) result).getValue())
							{
							sum =Long.parseLong(k1.get(z).toString());
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
			//lastKey = finalKey;
			}
			//for(String s:sumindexes.keySet()) {
			//	System.out.println("SUM INDEX:"+sumindexes.get(s));
			//}
		
		sitem = sitem +1;
		}
		}
	}
	}
	
	public static void getGroupByWO(MyTable table, List groupbyElementsList) throws SQLException {
		//System.out.println("INSIDE FUNCTION");
		//System.out.println("Hello:"+groupbyElementsList);
		ArrayList<String> colNames = new ArrayList<>();
		String row;
		ArrayList<String> ColumnNames =table.columnNames;
		ArrayList<ColDataType> datatypes =table.columnDatatype;
		ArrayList<ColumnDefinition> columns = table.tableColDef;
		ArrayList<Integer> indexes = new ArrayList<>();
		ArrayList<Integer> indexes2 = new ArrayList<>();
		//ArrayList<Integer> sumindex= new ArrayList<>();
		HashMap<String,ArrayList<Integer>> sumindexes  =  new HashMap<>();
 		
		//ArrayList<String> selectindexes =new ArrayList<>();
		int kt=0;
		ArrayList<ColumnDefinition> columns2 = new ArrayList<ColumnDefinition>();
		for(int i =0;i<ColumnNames.size();i++)
		{
			
			
			ColumnDefinition col = new ColumnDefinition();
			col.setColumnName(ColumnNames.get(i));
			col.setColDataType(datatypes.get(i));
			columns2.add(col);
		}
		//System.out.println("COLUMNS222"+columns2);
		//ArrayList<MyTable> tables = new ArrayList();
		//HashMap<Integer,MyTable> tableinfo =  new HashMap();
		/*SelectItem aggo=null;
		int i =0;
		int fi = 0;
		for(SelectItem si : ConfigureVariables.testselect) {
			//System.out.println("Select Item:"+si);
			if(si.toString().contains("SUM")) {
				aggo = si;
				fi =i;
				//System.out.println("FI:::"+fi);
				//System.out.println("AGGO::"+aggo);
				
			}
			i++;
				}*/
		int g = 0;
		
	
		
			
		
		/*for ( i=0;i<columns.size();i++) {
			 String colName = columns.get(i).getColumnName().toLowerCase();
			 ColumnNames.add(colName);
	         ColDataType colDataType = columns.get(i).getColDataType();
	         datatypes.add(colDataType);
		}*/
		
		//indexes of groupby columns
		for(Object o : groupbyElementsList) {
					int l = 0;
				for(String s : ColumnNames) {
						if(o.toString().toLowerCase().equals(s.toLowerCase())) {
							indexes.add(l);
						}
						l++;
					}
				
		}	
		/*
		for(SelectItem s:ConfigureVariables.testselect) {
			System.out.println("Select");
			System.out.println(s.toString());
		}
		*/
		
		for(SelectItem s:ConfigureVariables.testselect)
		{	
			int l=0;
			for(String s1 : ColumnNames) {
				
					 if(s.toString().toLowerCase().equals(s1.toLowerCase())) {
						 {	indexes2.add(l);
					break;}
						
				}
					 else {
						 if(s.toString().toLowerCase().contains("sum")) {
						 if(s.toString().toLowerCase().contains(s1.toString().toLowerCase())) {
							 indexes2.add(l);
							 break;
						 }}
					 }
				l++;
			}
		}
		//System.out.println("Indexes2:"+indexes2);
		//ArrayList<ColumnDefinition> newcd = new ArrayList<ColumnDefinition>();
		for(Integer h:indexes2)
		{
		ColumnDefinition col = new ColumnDefinition();
		
		col.setColumnName(ColumnNames.get(h));
		col.setColDataType(datatypes.get(h));
		//System.out.println("Heree:"+h +":"+ColumnNames.get(h));
		 ConfigureVariables.groupbycd.add(col);
		// System.out.println( "Grouped by" + ConfigureVariables.groupbycd.get(g));
		// g++;
		}
		for(ColumnDefinition c:ConfigureVariables.groupbycd) {
			for(SelectItem s:ConfigureVariables.testselect) {
				if(s.toString().toLowerCase().contains(c.getColumnName().toLowerCase()))
				{	
					String alias =((SelectExpressionItem) s).getAlias();;
					if(alias!=null) {
						{
							//System.out.println(alias+ "=alias");
						c.setColumnName(alias);
						}
					}
					
				}
			}
			
		}
		//System.out.println(ConfigureVariables.groupbycd+"LAALALALLAL");
		
		
		//ArrayList<String > med = new ArrayList<>();
		ArrayList<String> sit = new ArrayList<String>();
		for(SelectItem si : ConfigureVariables.testselect) {
			sit.add(si.toString());
		}
		
		if(ConfigureVariables.istpch1) {
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
				 hnm = nm.headMap(startdate, true);
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
				fr = new FileReader(ConfigureVariables.pathname+table.tableName+ConfigureVariables.FILEEXTENSION);
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
				
				PrimitiveValue tempRow[] = Tuple.getTupleVal(rowread, table.tableColDef);
				if(tempRow!=null) {
					//String tempRow[] = row.split("\\|");
					String finalKey="";
					for( int j=0; j< indexes.size(); j++) {
						String tempKey = tempRow[indexes.get(j)].toRawString();
						finalKey += tempKey + "|";
						
						
					}
					
					if(!sumindexes.containsKey(finalKey))
					{
						ArrayList<Integer> s= new ArrayList();
						for(int k =0;k<ConfigureVariables.testselect.size();k++)
						{
							s.add(0);
						}
						sumindexes.put(finalKey,s);
					}
					//System.out.println("Lastkey:"+"value"+lastKey);
					//System.out.println("Finalkey:"+"value"+finalKey);
					//if(finalKey.equals(lastKey)) {
						
					//System.out.println("final");
					int z=0;
					if(!ConfigureVariables.computedGroupByo.containsKey(finalKey))
					{
						ArrayList<Object> med = new ArrayList<>();
						for(int k =0;k<ConfigureVariables.testselect.size();k++)
						{
							med.add(null);
						}
						ConfigureVariables.computedGroupByo.put(finalKey,med);
					}
					int sitem=0;
					for(SelectItem si:ConfigureVariables.testselect) {
					//PrimitiveValue[] t = Tuple.getTupleVal(row, columns2);
					
					//if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
						//int k = ConfigureVariables.computedGroupBy.get(finalKey);
					
						//aggregate for already containing key
						String s = sit.get(sitem);
					if(!(s.contains("SUM")||s.contains("AVG")||s.contains("COUNT")||s.contains("MAX")||s.contains("MIN"))) {
						//!(si.toString().contains("SUM")||si.toString().contains("AVG")||si.toString().contains("COUNT")||si.toString().contains("MAX")||si.toString().contains("MIN"))
						if(si instanceof SelectExpressionItem) {
							Evaluator eval = new Evaluator(tempRow);
							try {
								PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
								//System.out.println(result.toRawString());
								if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
									ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
									k.set(z,result.toRawString());
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
									k.set(z,result.toRawString());
									
									 ConfigureVariables.computedGroupByo.put(finalKey, k);
									 z++;
								}
								
								
							} catch (SQLException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						
					}}
					else if(s.contains("SUM")) {
						
							//ConfigureVariables.isagg = true;
							Expression exp = ((SelectExpressionItem) si).getExpression();
							Function funct = (Function)exp;
							Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
							
							//System.out.println("Expression:"+exp1);
							Evaluator eval = new Evaluator(tempRow);
							
								PrimitiveValue result = eval.eval(exp1);
								//System.out.println(result.toRawString());
								if(result instanceof DoubleValue) {
									if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
										ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
										if(k.get(z)==null) {
											k.set(z, 0.0);
										}
										 double sum = ((Double) k.get(z)) + ((DoubleValue) result).getValue();
										k.set(z, sum);
										ConfigureVariables.computedGroupByo.put(finalKey,k);
										z++;
									
									}
									else  {
										ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
										k.set(z, result);
										 ConfigureVariables.computedGroupByo.put(finalKey, k);
										 z++;
									}
									
								}
								else if(result instanceof LongValue) {
									if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
										ArrayList<Object> k1 = ConfigureVariables.computedGroupByo.get(finalKey);
										
										if(k1.get(z)==null) {
											k1.set(z, 0);
										}
										Long sum =  ((LongValue) k1.get(z)).getValue() + ((LongValue) result).getValue();
										k1.set(z, sum);
										ConfigureVariables.computedGroupByo.put(finalKey, k1);
										z++;
									
									}
									else  {
										ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
										k.set(z, result);
										 ConfigureVariables.computedGroupByo.put(finalKey,k);
										 z++;
									}
								}
							}
					else if(s.contains("AVG")) {
						
						//ConfigureVariables.isagg = true;
						Expression exp = ((SelectExpressionItem) si).getExpression();
						Function funct = (Function)exp;
						Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
						
						//System.out.println("Expression:"+exp1);
						Evaluator eval = new Evaluator(tempRow);
						
							PrimitiveValue result = eval.eval(exp1);
							//System.out.println("RESULT:::"+result.toRawString());
							
							if(result instanceof DoubleValue) {
								if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
									//System.out.println("yes");
									ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
									//System.out.println("KKKKK"+k);
									if(k.get(z)==null) {
										k.set(z, 0.0);
									}
									Double t0 = ((Double)k.get(z))*sumindexes.get(finalKey).get(z);
									Double sum = t0 + ((DoubleValue) result).getValue();
									int l = sumindexes.get(finalKey).get(z);
									l= l+1;
									ArrayList o = sumindexes.get(finalKey);
									o.set(z,l);
									sumindexes.put(finalKey,o);
									Double avg= (double) (sum/l);
									k.set(z, avg);
									ConfigureVariables.computedGroupByo.put(finalKey,k);
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
									int l = sumindexes.get(finalKey).get(z);
									l=l+1;
									ArrayList o = sumindexes.get(finalKey);
									o.set(z,l);
									sumindexes.put(finalKey,o);
									Double avg=((DoubleValue) result).getValue()/l;
									k.set(z, avg);
									 ConfigureVariables.computedGroupByo.put(finalKey, k);
									 z++;
								}
								
							}
							else if(result instanceof LongValue) {
								if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
									ArrayList k1 = ConfigureVariables.computedGroupByo.get(finalKey);
									
									if(k1.get(z)==null) {
										k1.set(z, "0");
									}
									
									Long t0 = ((LongValue)  k1.get(z)).getValue()*sumindexes.get(finalKey).get(z);
									Long sum = t0 + ((LongValue) result).getValue();
									int l = sumindexes.get(finalKey).get(z);
									l= l+1;
									ArrayList o = sumindexes.get(finalKey);
									o.set(z,l);
									sumindexes.put(finalKey,o);
									Double avg= (double) (sum/l);
									//med.set(z, sum.toString());
									k1.set(z, avg);
									ConfigureVariables.computedGroupByo.put(finalKey, k1);
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
									int l = sumindexes.get(finalKey).get(z);
									l=l+1;
									ArrayList o = sumindexes.get(finalKey);
									o.set(z,l);
									sumindexes.put(finalKey,o);
									Double avg=((DoubleValue) result).getValue()/l;
									k.set(z, avg);
									 ConfigureVariables.computedGroupByo.put(finalKey,k);
									 z++;
								}
							}
						}
					else if(s.contains("COUNT")) {
						
						//ConfigureVariables.isagg = true;
						Expression exp = ((SelectExpressionItem) si).getExpression();
						//System.out.println(exp+"helllo");
						Function funct = (Function)exp;
						//System.out.println(funct+"helllo11111111");
						//Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
						
						//System.out.println("Expression:"+exp1);
						//Evaluator eval = new Evaluator(t,ConfigureVariables.dbMap);
						
						//	PrimitiveValue result = eval.eval(exp1);
							//System.out.println(result.toRawString());
							
								if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
									ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
									if(k.get(z)==null) {
										k.set(z, 0);
									}
									int sum =0;
									ArrayList o= sumindexes.get(finalKey);
									for(int j= 0;j<o.size();j++) {
										if((int) o.get(j)!=0) {
										sum =  (int) o.get(j);
										break;}
									}
									//Long add = (long) 1;
									//Long sum2 = Long.parseLong(;
									
									k.set(z, sum);
									ConfigureVariables.computedGroupByo.put(finalKey,k);
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
									System.out.println("here");
									k.set(z, 1);
									 ConfigureVariables.computedGroupByo.put(finalKey, k);
									 z++;
								}
							
						}
					else if(s.contains("MAX")) {
						
						//ConfigureVariables.isagg = true;
						Expression exp = ((SelectExpressionItem) si).getExpression();
						Function funct = (Function)exp;
						Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
						
						//System.out.println("Expression:"+exp1);
						Evaluator eval = new Evaluator(tempRow);
							PrimitiveValue result = eval.eval(exp1);
							//System.out.println(result.toRawString());
							if(result instanceof DoubleValue) {
								if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
									ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
									if(k.get(z)==null) {
										k.set(z, ((DoubleValue) result).getValue());
									}
									Double sum = 0.0;
									if(Double.parseDouble((String) k.get(z))>((DoubleValue) result).getValue())
									{
									sum =Double.parseDouble((String) k.get(z));
											}
									else
									{
									sum = ((DoubleValue) result).getValue()	;
									}
									k.set(z, sum);
									ConfigureVariables.computedGroupBy.put(finalKey,k);
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
									k.set(z, result);
									 ConfigureVariables.computedGroupBy.put(finalKey, k);
									 z++;
								}
								
							}
							else if(result instanceof LongValue) {
								if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
									ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
									
									if(k1.get(z)==null) {
										k1.set(z, ((LongValue) result).getValue());
									}
									Long sum = (long) 0;
									if(Long.parseLong((String) k1.get(z))>((LongValue) result).getValue())
									{
									sum =Long.parseLong((String) k1.get(z));
											}
									else
									{
									sum = ((LongValue) result).getValue()	;
									}
									k1.set(z, sum);
									ConfigureVariables.computedGroupBy.put(finalKey,k1);
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
									k.set(z, result);
									 ConfigureVariables.computedGroupBy.put(finalKey,k);
									 z++;
								}
							}
							
							
						//inside si
					}
						else if(s.contains("MIN")) {
						
						//ConfigureVariables.isagg = true;
						Expression exp = ((SelectExpressionItem) si).getExpression();
						Function funct = (Function)exp;
						Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
						
						//System.out.println("Expression:"+exp1);
						Evaluator eval = new Evaluator(tempRow);
						
							PrimitiveValue result = eval.eval(exp1);
							//System.out.println(result.toRawString());
							if(result instanceof DoubleValue) {
								if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
									ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
									if(k.get(z)==null) {
										k.set(z, ((DoubleValue) result).getValue());
									}
									Double sum = 0.0;
									if(Double.parseDouble((String) k.get(z))<((DoubleValue) result).getValue())
									{
									sum =Double.parseDouble((String) k.get(z));
											}
									else
									{
									sum = ((DoubleValue) result).getValue()	;
									}
									k.set(z, sum);
									ConfigureVariables.computedGroupBy.put(finalKey,k);
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
									k.set(z, result);
									 ConfigureVariables.computedGroupBy.put(finalKey, k);
									 z++;
								}
								
							}
							else if(result instanceof LongValue) {
								if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
									ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
									
									if(k1.get(z)==null) {
										k1.set(z, ((LongValue) result).getValue());
									}
									Long sum = (long) 0;
									if(Long.parseLong((String) k1.get(z))<((LongValue) result).getValue())
									{
									sum =Long.parseLong(k1.get(z).toString());
											}
									else
									{
									sum = ((LongValue) result).getValue()	;
									}
									k1.set(z, sum);
									ConfigureVariables.computedGroupBy.put(finalKey,k1);
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
									k.set(z, result);
									 ConfigureVariables.computedGroupBy.put(finalKey,k);
									 z++;
								}
							}
					//lastKey = finalKey;
					}
					//for(String s:sumindexes.keySet()) {
					//	System.out.println("SUM INDEX:"+sumindexes.get(s));
					//}
				
				sitem = sitem +1;
				}
				}
				long len = rowread.length() + 1;
				skip = (int) (len+i);
				
			
			}
			}catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		else {
			ArrayList<String> dval = new ArrayList<String>();
			if(ConfigureVariables.mapdelete.containsKey(table.tableName)){
				dval = ConfigureVariables.mapdelete.get(table.tableName);
				//System.out.println("yes contains deleted values");
				//System.out.println(dval);
			}
			//DELETE FROM LINEITEM WHERE ORDERKEY = 260;
		if(ConfigureVariables.mapdelete.containsKey(table.tableName)) {
			//String ds = "4414273|106108|6109|1|9.0|10026.9|0.03|0.06|R|F|1992-06-22|1992-06-30|1992-07-18|NONE|FOB|s. carefully pending accounts haggle q";
			//if(ConfigureVariables.mapdelete.get(table.tableName).contains(row))
			while((row = table.readtuple())!=null) {
				
			
				if(!dval.contains(row)){
				PrimitiveValue tempRow[] ;
				if(!ConfigureVariables.istpch1) {
				tempRow = checkSelection(row, table.tableColDef,table.tableName);
				}
				else {
					String srow[] = row.split("\\|");
					String trow[]= new String[11];
					for(int t =0;t<11;t++) {
						trow[t] = srow[t];
					}
					tempRow = Tuple.getTupleValSplittpch1(trow, table.tableColDef);
				}
				if(tempRow!=null) {
				//String tempRow[] = row.split("\\|");
				String finalKey="";
				for( int i=0; i< indexes.size(); i++) {
					String tempKey = tempRow[indexes.get(i)].toRawString();
					finalKey += tempKey + "|";
					
					
				}
				
				if(!sumindexes.containsKey(finalKey))
				{
					ArrayList<Integer> s= new ArrayList();
					for(int k =0;k<ConfigureVariables.testselect.size();k++)
					{
						s.add(0);
					}
					sumindexes.put(finalKey,s);
				}
				//System.out.println("Lastkey:"+"value"+lastKey);
				//System.out.println("Finalkey:"+"value"+finalKey);
				//if(finalKey.equals(lastKey)) {
					
				//System.out.println("final");
				int z=0;
				if(!ConfigureVariables.computedGroupByo.containsKey(finalKey))
				{
					ArrayList<Object> med = new ArrayList<>();
					for(int k =0;k<ConfigureVariables.testselect.size();k++)
					{
						med.add(null);
					}
					ConfigureVariables.computedGroupByo.put(finalKey,med);
				}
				int sitem=0;
				for(SelectItem si:ConfigureVariables.testselect) {
				//PrimitiveValue[] t = Tuple.getTupleVal(row, columns2);
				
				//if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
					//int k = ConfigureVariables.computedGroupBy.get(finalKey);
				
					//aggregate for already containing key
					String s = sit.get(sitem);
				if(!(s.contains("SUM")||s.contains("AVG")||s.contains("COUNT")||s.contains("MAX")||s.contains("MIN"))) {
					//!(si.toString().contains("SUM")||si.toString().contains("AVG")||si.toString().contains("COUNT")||si.toString().contains("MAX")||si.toString().contains("MIN"))
					if(si instanceof SelectExpressionItem) {
						Evaluator eval = new Evaluator(tempRow);
						try {
							PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
							//System.out.println(result.toRawString());
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								k.set(z,result.toRawString());
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								k.set(z,result.toRawString());
								
								 ConfigureVariables.computedGroupByo.put(finalKey, k);
								 z++;
							}
							
							
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					
				}}
				else if(s.contains("SUM")) {
					
						//ConfigureVariables.isagg = true;
						Expression exp = ((SelectExpressionItem) si).getExpression();
						Function funct = (Function)exp;
						Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
						
						//System.out.println("Expression:"+exp1);
						Evaluator eval = new Evaluator(tempRow);
						
							PrimitiveValue result = eval.eval(exp1);
							//System.out.println(result.toRawString());
							if(result instanceof DoubleValue) {
								if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
									ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
									if(k.get(z)==null) {
										k.set(z, 0.0);
									}
									 double sum = ((Double) k.get(z)) + ((DoubleValue) result).getValue();
									k.set(z, sum);
									ConfigureVariables.computedGroupByo.put(finalKey,k);
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
									k.set(z, result);
									 ConfigureVariables.computedGroupByo.put(finalKey, k);
									 z++;
								}
								
							}
							else if(result instanceof LongValue) {
								if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
									ArrayList<Object> k1 = ConfigureVariables.computedGroupByo.get(finalKey);
									
									if(k1.get(z)==null) {
										k1.set(z, 0);
									}
									Long sum =  ((LongValue) k1.get(z)).getValue() + ((LongValue) result).getValue();
									k1.set(z, sum);
									ConfigureVariables.computedGroupByo.put(finalKey, k1);
									z++;
								
								}
								else  {
									ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
									k.set(z, result);
									 ConfigureVariables.computedGroupByo.put(finalKey,k);
									 z++;
								}
							}
						}
				else if(s.contains("AVG")) {
					
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println("Expression:"+exp1);
					Evaluator eval = new Evaluator(tempRow);
					
						PrimitiveValue result = eval.eval(exp1);
						//System.out.println("RESULT:::"+result.toRawString());
						
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								//System.out.println("yes");
								ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
								//System.out.println("KKKKK"+k);
								if(k.get(z)==null) {
									k.set(z, 0.0);
								}
								Double t0 = ((Double)k.get(z))*sumindexes.get(finalKey).get(z);
								Double sum = t0 + ((DoubleValue) result).getValue();
								int l = sumindexes.get(finalKey).get(z);
								l= l+1;
								ArrayList o = sumindexes.get(finalKey);
								o.set(z,l);
								sumindexes.put(finalKey,o);
								Double avg= (double) (sum/l);
								k.set(z, avg);
								ConfigureVariables.computedGroupByo.put(finalKey,k);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								int l = sumindexes.get(finalKey).get(z);
								l=l+1;
								ArrayList o = sumindexes.get(finalKey);
								o.set(z,l);
								sumindexes.put(finalKey,o);
								Double avg=((DoubleValue) result).getValue()/l;
								k.set(z, avg);
								 ConfigureVariables.computedGroupByo.put(finalKey, k);
								 z++;
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								ArrayList k1 = ConfigureVariables.computedGroupByo.get(finalKey);
								
								if(k1.get(z)==null) {
									k1.set(z, "0");
								}
								
								Long t0 = ((LongValue)  k1.get(z)).getValue()*sumindexes.get(finalKey).get(z);
								Long sum = t0 + ((LongValue) result).getValue();
								int l = sumindexes.get(finalKey).get(z);
								l= l+1;
								ArrayList o = sumindexes.get(finalKey);
								o.set(z,l);
								sumindexes.put(finalKey,o);
								Double avg= (double) (sum/l);
								//med.set(z, sum.toString());
								k1.set(z, avg);
								ConfigureVariables.computedGroupByo.put(finalKey, k1);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								int l = sumindexes.get(finalKey).get(z);
								l=l+1;
								ArrayList o = sumindexes.get(finalKey);
								o.set(z,l);
								sumindexes.put(finalKey,o);
								Double avg=((DoubleValue) result).getValue()/l;
								k.set(z, avg);
								 ConfigureVariables.computedGroupByo.put(finalKey,k);
								 z++;
							}
						}
					}
				else if(s.contains("COUNT")) {
					
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					//System.out.println(exp+"helllo");
					Function funct = (Function)exp;
					//System.out.println(funct+"helllo11111111");
					//Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println("Expression:"+exp1);
					//Evaluator eval = new Evaluator(t,ConfigureVariables.dbMap);
					
					//	PrimitiveValue result = eval.eval(exp1);
						//System.out.println(result.toRawString());
						
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								if(k.get(z)==null) {
									k.set(z, 0);
								}
								int sum =0;
								ArrayList o= sumindexes.get(finalKey);
								for(int i= 0;i<o.size();i++) {
									if((int) o.get(i)!=0) {
									sum =  (int) o.get(i);
									break;}
								}
								//Long add = (long) 1;
								//Long sum2 = Long.parseLong(;
								
								k.set(z, sum);
								ConfigureVariables.computedGroupByo.put(finalKey,k);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								System.out.println("here");
								k.set(z, 1);
								 ConfigureVariables.computedGroupByo.put(finalKey, k);
								 z++;
							}
						
					}
				else if(s.contains("MAX")) {
					
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println("Expression:"+exp1);
					Evaluator eval = new Evaluator(tempRow);
						PrimitiveValue result = eval.eval(exp1);
						//System.out.println(result.toRawString());
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								if(k.get(z)==null) {
									k.set(z, ((DoubleValue) result).getValue());
								}
								Double sum = 0.0;
								if(Double.parseDouble((String) k.get(z))>((DoubleValue) result).getValue())
								{
								sum =Double.parseDouble((String) k.get(z));
										}
								else
								{
								sum = ((DoubleValue) result).getValue()	;
								}
								k.set(z, sum);
								ConfigureVariables.computedGroupBy.put(finalKey,k);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupBy.put(finalKey, k);
								 z++;
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
								
								if(k1.get(z)==null) {
									k1.set(z, ((LongValue) result).getValue());
								}
								Long sum = (long) 0;
								if(Long.parseLong((String) k1.get(z))>((LongValue) result).getValue())
								{
								sum =Long.parseLong((String) k1.get(z));
										}
								else
								{
								sum = ((LongValue) result).getValue()	;
								}
								k1.set(z, sum);
								ConfigureVariables.computedGroupBy.put(finalKey,k1);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupBy.put(finalKey,k);
								 z++;
							}
						}
						
						
					//inside si
				}
					else if(s.contains("MIN")) {
					
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println("Expression:"+exp1);
					Evaluator eval = new Evaluator(tempRow);
					
						PrimitiveValue result = eval.eval(exp1);
						//System.out.println(result.toRawString());
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								if(k.get(z)==null) {
									k.set(z, ((DoubleValue) result).getValue());
								}
								Double sum = 0.0;
								if(Double.parseDouble((String) k.get(z))<((DoubleValue) result).getValue())
								{
								sum =Double.parseDouble((String) k.get(z));
										}
								else
								{
								sum = ((DoubleValue) result).getValue()	;
								}
								k.set(z, sum);
								ConfigureVariables.computedGroupBy.put(finalKey,k);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupBy.put(finalKey, k);
								 z++;
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
								
								if(k1.get(z)==null) {
									k1.set(z, ((LongValue) result).getValue());
								}
								Long sum = (long) 0;
								if(Long.parseLong((String) k1.get(z))<((LongValue) result).getValue())
								{
								sum =Long.parseLong(k1.get(z).toString());
										}
								else
								{
								sum = ((LongValue) result).getValue()	;
								}
								k1.set(z, sum);
								ConfigureVariables.computedGroupBy.put(finalKey,k1);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupBy.put(finalKey,k);
								 z++;
							}
						}
				//lastKey = finalKey;
				}
				//for(String s:sumindexes.keySet()) {
				//	System.out.println("SUM INDEX:"+sumindexes.get(s));
				//}
			
			sitem = sitem +1;
			}
			}
		}
		}
		}
		
		else{
			
		while((row = table.readtuple())!=null) {
			PrimitiveValue tempRow[] ;
			if(!ConfigureVariables.istpch1) {
			tempRow = checkSelection(row, table.tableColDef,table.tableName);
			}
			else {
				String srow[] = row.split("\\|");
				String trow[]= new String[11];
				for(int t =0;t<11;t++) {
					trow[t] = srow[t];
				}
				tempRow = Tuple.getTupleValSplittpch1(trow, table.tableColDef);
			}
			if(tempRow!=null) {
			//String tempRow[] = row.split("\\|");
			String finalKey="";
			for( int i=0; i< indexes.size(); i++) {
				String tempKey = tempRow[indexes.get(i)].toRawString();
				finalKey += tempKey + "|";
				
				
			}
			
			if(!sumindexes.containsKey(finalKey))
			{
				ArrayList<Integer> s= new ArrayList();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					s.add(0);
				}
				sumindexes.put(finalKey,s);
			}
			//System.out.println("Lastkey:"+"value"+lastKey);
			//System.out.println("Finalkey:"+"value"+finalKey);
			//if(finalKey.equals(lastKey)) {
				
			//System.out.println("final");
			int z=0;
			if(!ConfigureVariables.computedGroupByo.containsKey(finalKey))
			{
				ArrayList<Object> med = new ArrayList<>();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					med.add(null);
				}
				ConfigureVariables.computedGroupByo.put(finalKey,med);
			}
			int sitem=0;
			for(SelectItem si:ConfigureVariables.testselect) {
			//PrimitiveValue[] t = Tuple.getTupleVal(row, columns2);
			
			//if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
				//int k = ConfigureVariables.computedGroupBy.get(finalKey);
			
				//aggregate for already containing key
				String s = sit.get(sitem);
			if(!(s.contains("SUM")||s.contains("AVG")||s.contains("COUNT")||s.contains("MAX")||s.contains("MIN"))) {
				//!(si.toString().contains("SUM")||si.toString().contains("AVG")||si.toString().contains("COUNT")||si.toString().contains("MAX")||si.toString().contains("MIN"))
				if(si instanceof SelectExpressionItem) {
					Evaluator eval = new Evaluator(tempRow);
					try {
						PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
						//System.out.println(result.toRawString());
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							k.set(z,result.toRawString());
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							k.set(z,result.toRawString());
							
							 ConfigureVariables.computedGroupByo.put(finalKey, k);
							 z++;
						}
						
						
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
			}}
			else if(s.contains("SUM")) {
				
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println("Expression:"+exp1);
					Evaluator eval = new Evaluator(tempRow);
					
						PrimitiveValue result = eval.eval(exp1);
						//System.out.println(result.toRawString());
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
								if(k.get(z)==null) {
									k.set(z, 0.0);
								}
								 double sum = ((Double) k.get(z)) + ((DoubleValue) result).getValue();
								k.set(z, sum);
								ConfigureVariables.computedGroupByo.put(finalKey,k);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupByo.put(finalKey, k);
								 z++;
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								ArrayList<Object> k1 = ConfigureVariables.computedGroupByo.get(finalKey);
								
								if(k1.get(z)==null) {
									k1.set(z, 0);
								}
								Long sum =  ((LongValue) k1.get(z)).getValue() + ((LongValue) result).getValue();
								k1.set(z, sum);
								ConfigureVariables.computedGroupByo.put(finalKey, k1);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupByo.put(finalKey,k);
								 z++;
							}
						}
					}
			else if(s.contains("AVG")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println("RESULT:::"+result.toRawString());
					
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							//System.out.println("yes");
							ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
							//System.out.println("KKKKK"+k);
							if(k.get(z)==null) {
								k.set(z, 0.0);
							}
							Double t0 = ((Double)k.get(z))*sumindexes.get(finalKey).get(z);
							Double sum = t0 + ((DoubleValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							k.set(z, avg);
							ConfigureVariables.computedGroupByo.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg);
							 ConfigureVariables.computedGroupByo.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupByo.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, "0");
							}
							
							Long t0 = ((LongValue)  k1.get(z)).getValue()*sumindexes.get(finalKey).get(z);
							Long sum = t0 + ((LongValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							//med.set(z, sum.toString());
							k1.set(z, avg);
							ConfigureVariables.computedGroupByo.put(finalKey, k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg);
							 ConfigureVariables.computedGroupByo.put(finalKey,k);
							 z++;
						}
					}
				}
			else if(s.contains("COUNT")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				//System.out.println(exp+"helllo");
				Function funct = (Function)exp;
				//System.out.println(funct+"helllo11111111");
				//Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				//Evaluator eval = new Evaluator(t,ConfigureVariables.dbMap);
				
				//	PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, 0);
							}
							int sum =0;
							ArrayList o= sumindexes.get(finalKey);
							for(int i= 0;i<o.size();i++) {
								if((int) o.get(i)!=0) {
								sum =  (int) o.get(i);
								break;}
							}
							//Long add = (long) 1;
							//Long sum2 = Long.parseLong(;
							
							k.set(z, sum);
							ConfigureVariables.computedGroupByo.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							System.out.println("here");
							k.set(z, 1);
							 ConfigureVariables.computedGroupByo.put(finalKey, k);
							 z++;
						}
					
				}
			else if(s.contains("MAX")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble((String) k.get(z))>((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble((String) k.get(z));
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong((String) k1.get(z))>((LongValue) result).getValue())
							{
							sum =Long.parseLong((String) k1.get(z));
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
					
					
				//inside si
			}
				else if(s.contains("MIN")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble((String) k.get(z))<((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble((String) k.get(z));
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong((String) k1.get(z))<((LongValue) result).getValue())
							{
							sum =Long.parseLong(k1.get(z).toString());
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
			//lastKey = finalKey;
			}
			//for(String s:sumindexes.keySet()) {
			//	System.out.println("SUM INDEX:"+sumindexes.get(s));
			//}
		
		sitem = sitem +1;
		}
		}
	}
		}
		if(ConfigureVariables.mapinsert.containsKey(table.tableName)) {
			
			for(String str1 : ConfigureVariables.mapinsert.get(table.tableName))
			{
				row = str1;
			PrimitiveValue tempRow[] ;
			if(!ConfigureVariables.istpch1) {
			tempRow = checkSelection(row, table.tableColDef,table.tableName);
			}
			else {
				String srow[] = row.split("\\|");
				String trow[]= new String[11];
				for(int t =0;t<11;t++) {
					trow[t] = srow[t];
				}
				tempRow = Tuple.getTupleValSplittpch1(trow, table.tableColDef);
			}
			if(tempRow!=null) {
			//String tempRow[] = row.split("\\|");
			String finalKey="";
			for( int i=0; i< indexes.size(); i++) {
				String tempKey = tempRow[indexes.get(i)].toRawString();
				finalKey += tempKey + "|";
				
				
			}
			
			if(!sumindexes.containsKey(finalKey))
			{
				ArrayList<Integer> s= new ArrayList();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					s.add(0);
				}
				sumindexes.put(finalKey,s);
			}
			//System.out.println("Lastkey:"+"value"+lastKey);
			//System.out.println("Finalkey:"+"value"+finalKey);
			//if(finalKey.equals(lastKey)) {
				
			//System.out.println("final");
			int z=0;
			if(!ConfigureVariables.computedGroupByo.containsKey(finalKey))
			{
				ArrayList<Object> med = new ArrayList<>();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					med.add(null);
				}
				ConfigureVariables.computedGroupByo.put(finalKey,med);
			}
			int sitem=0;
			for(SelectItem si:ConfigureVariables.testselect) {
			//PrimitiveValue[] t = Tuple.getTupleVal(row, columns2);
			
			//if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
				//int k = ConfigureVariables.computedGroupBy.get(finalKey);
			
				//aggregate for already containing key
				String s = sit.get(sitem);
			if(!(s.contains("SUM")||s.contains("AVG")||s.contains("COUNT")||s.contains("MAX")||s.contains("MIN"))) {
				//!(si.toString().contains("SUM")||si.toString().contains("AVG")||si.toString().contains("COUNT")||si.toString().contains("MAX")||si.toString().contains("MIN"))
				if(si instanceof SelectExpressionItem) {
					Evaluator eval = new Evaluator(tempRow);
					try {
						PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
						//System.out.println(result.toRawString());
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							k.set(z,result.toRawString());
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							k.set(z,result.toRawString());
							
							 ConfigureVariables.computedGroupByo.put(finalKey, k);
							 z++;
						}
						
						
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
			}}
			else if(s.contains("SUM")) {
				
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println("Expression:"+exp1);
					Evaluator eval = new Evaluator(tempRow);
					
						PrimitiveValue result = eval.eval(exp1);
						//System.out.println(result.toRawString());
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
								if(k.get(z)==null) {
									k.set(z, 0.0);
								}
								 double sum = ((Double) k.get(z)) + ((DoubleValue) result).getValue();
								k.set(z, sum);
								ConfigureVariables.computedGroupByo.put(finalKey,k);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupByo.put(finalKey, k);
								 z++;
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								ArrayList<Object> k1 = ConfigureVariables.computedGroupByo.get(finalKey);
								
								if(k1.get(z)==null) {
									k1.set(z, 0);
								}
								Long sum =  ((LongValue) k1.get(z)).getValue() + ((LongValue) result).getValue();
								k1.set(z, sum);
								ConfigureVariables.computedGroupByo.put(finalKey, k1);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupByo.put(finalKey,k);
								 z++;
							}
						}
					}
			else if(s.contains("AVG")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println("RESULT:::"+result.toRawString());
					
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							//System.out.println("yes");
							ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
							//System.out.println("KKKKK"+k);
							if(k.get(z)==null) {
								k.set(z, 0.0);
							}
							Double t0 = ((Double)k.get(z))*sumindexes.get(finalKey).get(z);
							Double sum = t0 + ((DoubleValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							k.set(z, avg);
							ConfigureVariables.computedGroupByo.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg);
							 ConfigureVariables.computedGroupByo.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupByo.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, "0");
							}
							
							Long t0 = ((LongValue)  k1.get(z)).getValue()*sumindexes.get(finalKey).get(z);
							Long sum = t0 + ((LongValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							//med.set(z, sum.toString());
							k1.set(z, avg);
							ConfigureVariables.computedGroupByo.put(finalKey, k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg);
							 ConfigureVariables.computedGroupByo.put(finalKey,k);
							 z++;
						}
					}
				}
			else if(s.contains("COUNT")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				//System.out.println(exp+"helllo");
				Function funct = (Function)exp;
				//System.out.println(funct+"helllo11111111");
				//Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				//Evaluator eval = new Evaluator(t,ConfigureVariables.dbMap);
				
				//	PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, 0);
							}
							int sum =0;
							ArrayList o= sumindexes.get(finalKey);
							for(int i= 0;i<o.size();i++) {
								if((int) o.get(i)!=0) {
								sum =  (int) o.get(i);
								break;}
							}
							//Long add = (long) 1;
							//Long sum2 = Long.parseLong(;
							
							k.set(z, sum);
							ConfigureVariables.computedGroupByo.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							//System.out.println("here");
							k.set(z, 1);
							 ConfigureVariables.computedGroupByo.put(finalKey, k);
							 z++;
						}
					
				}
			else if(s.contains("MAX")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble((String) k.get(z))>((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble((String) k.get(z));
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong((String) k1.get(z))>((LongValue) result).getValue())
							{
							sum =Long.parseLong((String) k1.get(z));
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
					
					
				//inside si
			}
				else if(s.contains("MIN")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble((String) k.get(z))<((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble((String) k.get(z));
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong((String) k1.get(z))<((LongValue) result).getValue())
							{
							sum =Long.parseLong(k1.get(z).toString());
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
			//lastKey = finalKey;
			}
			//for(String s:sumindexes.keySet()) {
			//	System.out.println("SUM INDEX:"+sumindexes.get(s));
			//}
		
		sitem = sitem +1;
		}
		}
			
		}
		}
	}
	}
	
	public static void getGroupByJO(MyTable table, List groupbyElementsList) throws SQLException {
		//System.out.println("INSIDE FUNCTION");
		//System.out.println("Hello:"+groupbyElementsList);
		ArrayList<String> colNames = new ArrayList<>();
		String row;
		ArrayList<String> ColumnNames =table.columnNames;
		ArrayList<ColDataType> datatypes =table.columnDatatype;
		ArrayList<ColumnDefinition> columns = table.tableColDef;
		ArrayList<Integer> indexes = new ArrayList<>();
		ArrayList<Integer> indexes2 = new ArrayList<>();
		//ArrayList<Integer> sumindex= new ArrayList<>();
		HashMap<String,ArrayList<Integer>> sumindexes  =  new HashMap<>();
		ArrayList<String> rows = table.tuples;
		
		//System.out.println("SIZE of final table"+ rows.size());
		//ArrayList<String> selectindexes =new ArrayList<>();
		int kt=0;
		ArrayList<ColumnDefinition> columns2 = new ArrayList<ColumnDefinition>();
		for(int i =0;i<ColumnNames.size();i++)
		{
			
			
			ColumnDefinition col = new ColumnDefinition();
			col.setColumnName(ColumnNames.get(i));
			col.setColDataType(datatypes.get(i));
			columns2.add(col);
		}
		//System.out.println("COLUMNS222"+columns2);
		//ArrayList<MyTable> tables = new ArrayList();
		//HashMap<Integer,MyTable> tableinfo =  new HashMap();
		/*SelectItem aggo=null;
		int i =0;
		int fi = 0;
		for(SelectItem si : ConfigureVariables.testselect) {
			//System.out.println("Select Item:"+si);
			if(si.toString().contains("SUM")) {
				aggo = si;
				fi =i;
				//System.out.println("FI:::"+fi);
				//System.out.println("AGGO::"+aggo);
				
			}
			i++;
				}*/
		int g = 0;
		
	
		
			
		
		/*for ( i=0;i<columns.size();i++) {
			 String colName = columns.get(i).getColumnName().toLowerCase();
			 ColumnNames.add(colName);
	         ColDataType colDataType = columns.get(i).getColDataType();
	         datatypes.add(colDataType);
		}*/
		
		//indexes of groupby columns
		for(Object o : groupbyElementsList) {
					int l = 0;
				for(String s : ColumnNames) {
						if(o.toString().toLowerCase().equals(s.toLowerCase())) {
							indexes.add(l);
						}
						l++;
					}
				
		}	
		/*
		for(SelectItem s:ConfigureVariables.testselect) {
			System.out.println("Select");
			System.out.println(s.toString());
		}
		*/
		
		for(SelectItem s:ConfigureVariables.testselect)
		{	
			int l=0;
			for(String s1 : ColumnNames) {
				
					 if(s.toString().toLowerCase().equals(s1.toLowerCase())) {
						 {	indexes2.add(l);
					break;}
						
				}
					 else {
						 if(s.toString().toLowerCase().contains("sum")) {
						 if(s.toString().toLowerCase().contains(s1.toString().toLowerCase())) {
							 indexes2.add(l);
							 break;
						 }}
					 }
				l++;
			}
		}
		//System.out.println("Indexes2:"+indexes2);
		//ArrayList<ColumnDefinition> newcd = new ArrayList<ColumnDefinition>();
		for(Integer h:indexes2)
		{
		ColumnDefinition col = new ColumnDefinition();
		
		col.setColumnName(ColumnNames.get(h));
		col.setColDataType(datatypes.get(h));
		//System.out.println("Heree:"+h +":"+ColumnNames.get(h));
		 ConfigureVariables.groupbycd.add(col);
		// System.out.println( "Grouped by" + ConfigureVariables.groupbycd.get(g));
		// g++;
		}
		for(ColumnDefinition c:ConfigureVariables.groupbycd) {
			for(SelectItem s:ConfigureVariables.testselect) {
				if(s.toString().toLowerCase().contains(c.getColumnName().toLowerCase()))
				{	
					String alias =((SelectExpressionItem) s).getAlias();;
					if(alias!=null) {
						{
							//System.out.println(alias+ "=alias");
						c.setColumnName(alias);
						}
					}
					
				}
			}
			
		}
		//System.out.println(ConfigureVariables.groupbycd+"LAALALALLAL");
		
		
		//ArrayList<String > med = new ArrayList<>();
		ArrayList<String> sit = new ArrayList<String>();
		for(SelectItem si : ConfigureVariables.testselect) {
			sit.add(si.toString());
		}
		int ind=0;
		//int size = rows.size() -1;
		int size = rows.size();
		while(ind < size) {
			row = rows.get(ind);
			ind = ind +1;
		//while((row = table.readtuple())!=null) {
			String tempR[] = row.split("\\|");
			String finalKey="";
			//tempRow = checkSelection(row, table.tableColDef,table.tableName);
			//System.out.println(row);
			PrimitiveValue tempRow[] = Tuple.getTupleValSplit(tempR, table.tableColDef);
			if(tempRow!=null) {
			//String tempRow[] = row.split("\\|");
			
			for( int i=0; i< indexes.size(); i++) {
				String tempKey = tempRow[indexes.get(i)].toRawString();
				finalKey += tempKey + "|";
				
				
			}
			
			if(!sumindexes.containsKey(finalKey))
			{
				ArrayList<Integer> s= new ArrayList();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					s.add(0);
				}
				sumindexes.put(finalKey,s);
			}
			//System.out.println("Lastkey:"+"value"+lastKey);
			//System.out.println("Finalkey:"+"value"+finalKey);
			//if(finalKey.equals(lastKey)) {
				
			//System.out.println("final");
			int z=0;
			if(!ConfigureVariables.computedGroupByo.containsKey(finalKey))
			{
				ArrayList<Object> med = new ArrayList<>();
				for(int k =0;k<ConfigureVariables.testselect.size();k++)
				{
					med.add(null);
				}
				ConfigureVariables.computedGroupByo.put(finalKey,med);
			}
			int sitem=0;
			for(SelectItem si:ConfigureVariables.testselect) {
			//PrimitiveValue[] t = Tuple.getTupleVal(row, columns2);
			
			//if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
				//int k = ConfigureVariables.computedGroupBy.get(finalKey);
			
				//aggregate for already containing key
				String s = sit.get(sitem);
			if(!(s.contains("SUM")||s.contains("AVG")||s.contains("COUNT")||s.contains("MAX")||s.contains("MIN"))) {
				//!(si.toString().contains("SUM")||si.toString().contains("AVG")||si.toString().contains("COUNT")||si.toString().contains("MAX")||si.toString().contains("MIN"))
				if(si instanceof SelectExpressionItem) {
					Evaluator eval = new Evaluator(tempRow);
					try {
						PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
						//System.out.println(result.toRawString());
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							k.set(z,result.toRawString());
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							k.set(z,result.toRawString());
							
							 ConfigureVariables.computedGroupByo.put(finalKey, k);
							 z++;
						}
						
						
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
			}}
			else if(s.contains("SUM")) {
				
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println("Expression:"+exp1);
					Evaluator eval = new Evaluator(tempRow);
					
						PrimitiveValue result = eval.eval(exp1);
						//System.out.println(result.toRawString());
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
								if(k.get(z)==null) {
									k.set(z, 0.0);
								}
								 double sum = ((Double) k.get(z)) + ((DoubleValue) result).getValue();
								k.set(z, sum);
								ConfigureVariables.computedGroupByo.put(finalKey,k);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupByo.put(finalKey, k);
								 z++;
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
								ArrayList<Object> k1 = ConfigureVariables.computedGroupByo.get(finalKey);
								
								if(k1.get(z)==null) {
									k1.set(z, 0L);
								}
								Long sum =  ((Long) k1.get(z)) + ((LongValue) result).getValue();
								k1.set(z, sum);
								ConfigureVariables.computedGroupByo.put(finalKey, k1);
								z++;
							
							}
							else  {
								ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
								k.set(z, result);
								 ConfigureVariables.computedGroupByo.put(finalKey,k);
								 z++;
							}
						}
					}
			else if(s.contains("AVG")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println("RESULT:::"+result.toRawString());
					
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							//System.out.println("yes");
							ArrayList<Object> k = ConfigureVariables.computedGroupByo.get(finalKey);
							//System.out.println("KKKKK"+k);
							if(k.get(z)==null) {
								k.set(z, 0.0);
							}
							Double t0 = ((Double)k.get(z))*sumindexes.get(finalKey).get(z);
							Double sum = t0 + ((DoubleValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							k.set(z, avg);
							ConfigureVariables.computedGroupByo.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg);
							 ConfigureVariables.computedGroupByo.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupByo.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, "0");
							}
							
							Long t0 = ((LongValue)  k1.get(z)).getValue()*sumindexes.get(finalKey).get(z);
							Long sum = t0 + ((LongValue) result).getValue();
							int l = sumindexes.get(finalKey).get(z);
							l= l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg= (double) (sum/l);
							//med.set(z, sum.toString());
							k1.set(z, avg);
							ConfigureVariables.computedGroupByo.put(finalKey, k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							int l = sumindexes.get(finalKey).get(z);
							l=l+1;
							ArrayList o = sumindexes.get(finalKey);
							o.set(z,l);
							sumindexes.put(finalKey,o);
							Double avg=((DoubleValue) result).getValue()/l;
							k.set(z, avg);
							 ConfigureVariables.computedGroupByo.put(finalKey,k);
							 z++;
						}
					}
				}
			else if(s.contains("COUNT")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				//System.out.println(exp+"helllo");
				Function funct = (Function)exp;
				//System.out.println(funct+"helllo11111111");
				//Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				//Evaluator eval = new Evaluator(t,ConfigureVariables.dbMap);
				
				//	PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					
						if(ConfigureVariables.computedGroupByo.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, 0);
							}
							int sum =0;
							ArrayList o= sumindexes.get(finalKey);
							for(int i= 0;i<o.size();i++) {
								if((int) o.get(i)!=0) {
								sum =  (int) o.get(i);
								break;}
							}
							//Long add = (long) 1;
							//Long sum2 = Long.parseLong(;
							
							k.set(z, sum);
							ConfigureVariables.computedGroupByo.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupByo.get(finalKey);
							//System.out.println("here");
							k.set(z, 1);
							 ConfigureVariables.computedGroupByo.put(finalKey, k);
							 z++;
						}
					
				}
			else if(s.contains("MAX")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble((String) k.get(z))>((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble((String) k.get(z));
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong((String) k1.get(z))>((LongValue) result).getValue())
							{
							sum =Long.parseLong((String) k1.get(z));
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
					
					
				//inside si
			}
				else if(s.contains("MIN")) {
				
				//ConfigureVariables.isagg = true;
				Expression exp = ((SelectExpressionItem) si).getExpression();
				Function funct = (Function)exp;
				Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
				
				//System.out.println("Expression:"+exp1);
				Evaluator eval = new Evaluator(tempRow);
				
					PrimitiveValue result = eval.eval(exp1);
					//System.out.println(result.toRawString());
					if(result instanceof DoubleValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							if(k.get(z)==null) {
								k.set(z, ((DoubleValue) result).getValue());
							}
							Double sum = 0.0;
							if(Double.parseDouble((String) k.get(z))<((DoubleValue) result).getValue())
							{
							sum =Double.parseDouble((String) k.get(z));
									}
							else
							{
							sum = ((DoubleValue) result).getValue()	;
							}
							k.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey, k);
							 z++;
						}
						
					}
					else if(result instanceof LongValue) {
						if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
							ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);
							
							if(k1.get(z)==null) {
								k1.set(z, ((LongValue) result).getValue());
							}
							Long sum = (long) 0;
							if(Long.parseLong((String) k1.get(z))<((LongValue) result).getValue())
							{
							sum =Long.parseLong(k1.get(z).toString());
									}
							else
							{
							sum = ((LongValue) result).getValue()	;
							}
							k1.set(z, sum);
							ConfigureVariables.computedGroupBy.put(finalKey,k1);
							z++;
						
						}
						else  {
							ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
							k.set(z, result);
							 ConfigureVariables.computedGroupBy.put(finalKey,k);
							 z++;
						}
					}
			//lastKey = finalKey;
			}
			//for(String s:sumindexes.keySet()) {
			//	System.out.println("SUM INDEX:"+sumindexes.get(s));
			//}
		
		sitem = sitem +1;
		}
		}
	}
	}
	
	public static PrimitiveValue[] checkSelection(String str, ArrayList<ColumnDefinition> columnDefinitionList, String tableName) {
		//System.out.println("Check sel");
		PrimitiveValue t[];
		t = Tuple.getTupleVal(str, columnDefinitionList);
		
		Evaluator eval = new Evaluator(t);
		ArrayList<Expression> tc = ConfigureVariables.tableCondition.get(tableName);
		//System.out.println("Conditions::"+tc);
		if(tc==null) {
			return t;
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
						 return null;
					 }
					 if(count == tc.size()) {
						 return t;
					 }
					 
				} catch (SQLException e) {
					
					e.printStackTrace();
				}	
			
			}
			
			
		}
		
		else {
			return t;
		}
		
		
		return t; //Default return type ( Not in use).
		
	}


}
	

			

	

		
		


