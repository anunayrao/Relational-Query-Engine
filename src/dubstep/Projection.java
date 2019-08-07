package dubstep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
////@Author - Anunay Rao

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
public class Projection {

	
	
		public static void project(ArrayList<String> list) {
			
			if(ConfigureVariables.limit!=0) {
				
				for(int i=0;i<ConfigureVariables.limit;i++)
				{
					System.out.println(list.get(i));
				}
			}
			else {
				for(int i=0; i<list.size();i++) {
					System.out.println(list.get(i));
				}
				//System.out.println("FILES::"+ConfigureVariables.files);
			}
			//System.out.println("After join columns:"+ConfigureVariables.afterjoincol);
			//long endtime = System.currentTimeMillis();
			//long e = endtime - ConfigureVariables.starttime;
			
			//System.out.println("Execution time in milliseconds for query : " + e);
			//System.out.println("COUNT:"+ConfigureVariables.countcheck);
			//System.out.println("SIZE:::"+ConfigureVariables.countcheck);
			Projection.deletefiles();
			ConfigureVariables.reset();
			
			
			//delete files and reset
		}
		
		public static void projectfile(String filename) throws IOException {
			try {
				FileReader fr = new FileReader(ConfigureVariables.temppath+filename+ConfigureVariables.FILEEXTENSION);
				BufferedReader br = new BufferedReader(fr);
				
				if(ConfigureVariables.limit!=0) {
					for(int i =0; i<ConfigureVariables.limit;i++) {
						try {
							String str = br.readLine();
							System.out.println(str);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
				}
				else {
					String str;
					while((str = br.readLine())!=null) {
						System.out.println(str);
					}
				}
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//
			//long endtime = System.currentTimeMillis();
			//long e = endtime - ConfigureVariables.starttime;
			
			//System.out.println("Execution time in milliseconds for query : " + e);
			Projection.deletefiles();
			ConfigureVariables.reset();
			
			//delete files and reset
		}
		
		public static void projectdataobject(MyTable table) {
			
			if(ConfigureVariables.limit!=0) {
				for(int i =0 ;i<ConfigureVariables.limit;i++) {
					String str = table.readtuple();
					System.out.println(str);
				}
			}
			else {
				String str;
				while((str=table.readtuple())!=null) {
					System.out.println(str);
				}
			}
			//
			//long endtime = System.currentTimeMillis();
			//long e = endtime - ConfigureVariables.starttime;
			
			//System.out.println("Execution time in milliseconds for query : " + e);
			Projection.deletefiles();
			ConfigureVariables.reset();
			
			//delete files and reset
		}
		
		public static void projectdataobjectsingle(MyTable table) {
			
				String str;
				//System.out.println("youuu"+table.readtuple());
				while((str=table.readtuple())!=null) {
					//System.out.println(str);
					PrimitiveValue []t = Tuple.getTupleVal(str,table.tableColDef);
			Evaluator eval = new Evaluator(t);
			int i=0;
			for(SelectItem si : ConfigureVariables.testselect) {
			try {
				PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
				if(i<ConfigureVariables.testselect.size()-1)
				System.out.print(result.toRawString()+"|");
				else
					System.out.print(result.toRawString());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			i++;
		}
			System.out.println();
			if(ConfigureVariables.limit==i) {
				break;
			}
			}
				//long endtime = System.currentTimeMillis();
				//long e = endtime - ConfigureVariables.starttime;
				
				//System.out.println("Execution time in milliseconds for query : " + e);
			
			//
			Projection.deletefiles();
			ConfigureVariables.reset();
			
			//delete files and reset
		}
		
		
		
		
		
		public static void projectdataobjectsingleagg(MyTable table) 
		{	
			String tableName = table.tableName;
			ArrayList<ColumnDefinition> cd = table.tableColDef;
			ArrayList<String>  str1 = new ArrayList<String>();
			for(int i=0;i<cd.size();i++) {
				str1.add(cd.get(i).getColumnName());
			}
			//System.out.println("STR11111"+str1);
			
			String str;
			while((str=table.readtuple())!=null) {
				//System.out.println(str);
				PrimitiveValue []t = Tuple.getTupleVal(str,table.tableColDef);
				//Evaluator eval = new Evaluator(t);
			
			
			int i =0;
			
			for(SelectItem si : ConfigureVariables.testselect) {
				//System.out.println("c:"+si);
				if(si instanceof AllColumns || (si.toString().equals(ConfigureVariables.tablealias.get(tableName.toUpperCase())+".*")) ){
					for ( int a =0; a<str1.size();a++) {
		        		//System.out.println(str.get(a));
		        		//System.out.println("S:"+s);
		        			if((a==str1.size()-1)&&(i == ConfigureVariables.testselect.size()-1)) {
		        				System.out.print(t[a].toRawString());
		        				
		        			}
		        			else {
		        			System.out.print(t[a].toRawString()+"|");
		        			
		        			}
		        		
		        	}
					i++;
				}
				else if(si.toString().contains("MIN")) {
					ConfigureVariables.isagg = true;
					
					
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println(exp1);
					Evaluator eval = new Evaluator(t);
					try {
						PrimitiveValue result = eval.eval(exp1);
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.firsttuple) {
								ConfigureVariables.res.add(result.toRawString());
								
							}
							else if(Double.parseDouble(ConfigureVariables.res.get(i)) > ((DoubleValue) result).getValue()) {
								ConfigureVariables.res.set(i,result.toRawString());
								i++;
							}
							else
								i++;
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.firsttuple) {
								ConfigureVariables.res.add(result.toRawString());
								
							}
							else if(Long.parseLong(ConfigureVariables.res.get(i)) > ((LongValue) result).getValue()) {
								ConfigureVariables.res.set(i,result.toRawString());
								i++;
							}
							else
								i++;
						}
						//System.out.println(result.toRawString());
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
				}
				else if(si.toString().contains("MAX")) {
					ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println(exp1);
					Evaluator eval = new Evaluator(t);
					try {
						PrimitiveValue result = eval.eval(exp1);
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.firsttuple) {
								ConfigureVariables.res.add(result.toRawString());
								
							}
							else if(Double.parseDouble(ConfigureVariables.res.get(i)) < ((DoubleValue) result).getValue()) {
								ConfigureVariables.res.set(i,result.toRawString());
								i++;
							}
							else
								i++;
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.firsttuple) {
								ConfigureVariables.res.add(result.toRawString());
								
							}
							else if(Long.parseLong(ConfigureVariables.res.get(i)) < ((LongValue) result).getValue()) {
								ConfigureVariables.res.set(i,result.toRawString());
								i++;
							}
							else
								i++;
						}
						//System.out.println(result.toRawString());
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
				}
				else if(si.toString().contains("SUM")) {
					//System.out.println("yes");
					ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) si).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println(exp1);
					Evaluator eval = new Evaluator(t);
					try {
						PrimitiveValue result = eval.eval(exp1);
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.firsttuple) {
								ConfigureVariables.res.add(result.toRawString());
								
							}
							else  {
								Double s = Double.parseDouble(ConfigureVariables.res.get(i)) + ((DoubleValue) result).getValue();
								ConfigureVariables.res.set(i,s.toString());
								i++;
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.firsttuple) {
								ConfigureVariables.res.add(result.toRawString());
								
							}
							else  {
								Long s = Long.parseLong(ConfigureVariables.res.get(i)) + ((LongValue) result).getValue();
								ConfigureVariables.res.set(i,s.toString());
								i++;
							}
						}
						//System.out.println(result.toRawString());
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
				}
				else if(si.toString().contains("COUNT(*)")) {
					ConfigureVariables.isagg = true;
					
					//System.out.println(exp1);
					
							if(ConfigureVariables.firsttuple) {
								ConfigureVariables.res.add(Integer.toString(1));
								
							}
							else {
								Integer l = Integer.parseInt(ConfigureVariables.res.get(i));
								
								ConfigureVariables.res.set(i,Integer.toString(l+1));
								i++;
							}
							
							
						
					
				}
				
				else if(si instanceof SelectExpressionItem) {
					
					Evaluator eval = new Evaluator(t);
					try {
						PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
						if(i<ConfigureVariables.testselect.size()-1)
						System.out.print(result.toRawString()+"|");
						else
							System.out.print(result.toRawString());
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					i++;
				}
				
				
				
			}
			ConfigureVariables.firsttuple = false;
			if(!ConfigureVariables.isagg)
			System.out.println();
			}
		
		}
		
		/*
		public static void proj(MyTable table) {
			String str;
			while((str=table.readtuple())!=null) {
				System.out.println(str);
				PrimitiveValue []t = Tuple.getTupleVal(str,table.tableColDef);
				
				ProjectionOperator.Projection(table.tableName.toLowerCase(), t,table.tableColDef);
			}
			
		}
		*/
		public static void deletefiles() {
			//System.out.println("CALLED");
			//System.out.println(ConfigureVariables.files.size());
			for(int i =0; i<ConfigureVariables.files.size();i++) {
			
			File f = new File(ConfigureVariables.temppath+ConfigureVariables.files.get(i));
			//System.out.println("Checking::"+ConfigureVariables.files.get(i));
			if(f.exists()) {
				//System.out.println("EXISTS and DELETED");
				f.delete();
			}
			}
		}
}
