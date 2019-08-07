package dubstep;
////@Author - Anunay Rao
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class TableScan {

	static PrimitiveValue []ot ;
	static PrimitiveValue []it ;
	static PrimitiveValue []t;
	static int i =0;
	static ArrayList<PrimitiveValue[]> tuples = new ArrayList(); 
	public static void SimpleScan(MyTable table, HashMap<String, BufferedReader> tablefile, ArrayList<String> tableStrings) {
		int i;
		//System.out.println("Hello" + tableStrings);
		for( i = 0; i < tableStrings.size();i++ ) {
			
		BufferedReader br1 = null ;
		//= ConfigureVariables.tablefile.get(ConfigureVariables.tableStrings.get(i).toLowerCase());
		try {
			br1 = new BufferedReader(new FileReader(ConfigureVariables.pathname + ConfigureVariables.tableStrings.get(i) + ConfigureVariables.FILEEXTENSION));
			//ConfigureVariables.tablefile.put(tableName,in);
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
		//if(br == null)
		//	System.out.println("NULLLLL");
		//System.out.println(tableStrings.get(0));
		//BufferedReader r = tablefile.get(tableStrings.get(0));
		/*
		try {
			
			System.out.println(r.readLine());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		
		/*
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(ConfigureVariables.pathname + "T.dat"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		
		if(ConfigureVariables.istpch6) {
			//System.out.println("Heelo");
			Date startdate,enddate;
			String sdate=null, edate=null;
			NavigableMap<Date, ArrayList<Long>> nm;
			NavigableMap<Date, ArrayList<Long>> hnm = null;;
			ArrayList<Expression> exp = ConfigureVariables.tableCondition.get("LINEITEM");
			ArrayList<Expression> fexp = new ArrayList<Expression>();
			ArrayList<Expression> newexp = new ArrayList<Expression>();
			for(Expression ex : exp ) {
				String sexp = ((BinaryExpression) ex).getLeftExpression().toString();
				//System.out.println(sexp);
				if(sexp.contains("LINEITEM.SHIPDATE")) {
					//System.out.println("yes");
					fexp.add(ex);
				}
				else {
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
				 nm = Main.readNMap("LINEITEM.ind");
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
			for(Long j : set) {
				//System.out.println(i);
				//System.out.println(skip);
				br.skip(j-skip);
				String rowread = br.readLine();
				//System.out.println(rowread);
				if(rowread==null) {
					break;
				}
				PrimitiveValue tuple[] = Tuple.getTupleVal(rowread, table.tableColDef);
				SelectionOperator.Selection(tuple,ConfigureVariables.tableStrings.get(i).toLowerCase());
				long len = rowread.length() + 1;
				skip = (int) (len+j);
				
			
			}
			int w=0;
			for(String s : ConfigureVariables.res) {
				if(w<ConfigureVariables.res.size()-1 ){
				System.out.print(s+"|");
				w++;
				}
				else
				System.out.print(s);
			}
			if(ConfigureVariables.isagg) {
				System.out.println();
			}
			Projection.deletefiles();
			//ConfigureVariables.reset();
			ConfigureVariables.reset();
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		}
		else {
			
			String line;
		    try {
		    	
				while ((line = br1.readLine()) != null) {
				    ConfigureVariables.deleterow = line;
					// process line here.
					//System.out.println(line);
					//List<String> items = Arrays.asList(line.split("\\s*\\|\\s*"));
					//System.out.println(items.get(0)+"|"+items.get(1));
					//t = Tuple.getTuple(line, ConfigureVariables.createTableMap,tableStrings.get(i));
					
					//Check for tuple if deleted
					//System.out.println("Name:"+table.tableName);
					
					if(ConfigureVariables.mapdelete.containsKey(table.tableName)){
						ArrayList<String> val = ConfigureVariables.mapdelete.get(table.tableName);
						//System.out.println("203:deleted"+val);
						if(!val.contains(line)) {
							t = Tuple.getTuple(line, ConfigureVariables.createTableMap,ConfigureVariables.tableStrings.get(i).toLowerCase(),table.tableColDef);
							SelectionOperator.Selection(t,ConfigureVariables.tableStrings.get(i).toLowerCase());
						}
						else {
							//System.out.println("Rejected:"+line);
						}
					}
					else {
						//System.out.println("No delete yet!!");
					t = Tuple.getTuple(line, ConfigureVariables.createTableMap,ConfigureVariables.tableStrings.get(i).toLowerCase(),table.tableColDef);
					
					//System.out.println("Tuple:"+ t);
					SelectionOperator.Selection(t,ConfigureVariables.tableStrings.get(i).toLowerCase());
					}
					}
				
				//CREATE TABLE Q(NAME STRING, AGE INT);
				//INSERT INTO Q(NAME,AGE) VALUES('ABHINAY',30);
				//INSERT INTO Q(NAME,AGE) VALUES('PAPA',63);
				//DELETE FROM Q WHERE AGE<70;
				//SELECT Q.NAME FROM Q;

				//Add Code to pass new inserted rows
				
				if(ConfigureVariables.mapinsert.containsKey(table.tableName)) {
					//System.out.println("for insert");
					if(ConfigureVariables.mapdelete.containsKey(table.tableName)) {
						ArrayList<String> dval = ConfigureVariables.mapdelete.get(table.tableName);
						//System.out.println("DVAL"+dval);
						ArrayList<String> val = ConfigureVariables.mapinsert.get(table.tableName);
						//System.out.println("val"+val);
						for(String s: val) {
							if(!dval.contains(s)) {
							t = Tuple.getTuple(s, ConfigureVariables.createTableMap,ConfigureVariables.tableStrings.get(i).toLowerCase(),table.tableColDef);
							SelectionOperator.Selection(t,ConfigureVariables.tableStrings.get(i).toLowerCase());
							}
						}
					}
					else {
					ArrayList<String> val = ConfigureVariables.mapinsert.get(table.tableName);
					for(String s: val) {
						//System.out.println("SSSSS:::"+s);
						t = Tuple.getTuple(s, ConfigureVariables.createTableMap,ConfigureVariables.tableStrings.get(i).toLowerCase(),table.tableColDef);
						SelectionOperator.Selection(t,ConfigureVariables.tableStrings.get(i).toLowerCase());
					}
					}
				}
				
				
				int w=0;
				for(String s : ConfigureVariables.res) {
					if(w<ConfigureVariables.res.size()-1 ){
					System.out.print(s+"|");
					w++;
					}
					else
					System.out.print(s);
				}
				if(ConfigureVariables.isagg) {
					System.out.println();
				}
				Projection.deletefiles();
				//ConfigureVariables.reset();
				if(!ConfigureVariables.delete) {
				ConfigureVariables.reset();
				}
					//br = new BufferedReader(new FileReader(ConfigureVariables.pathname + ConfigureVariables.tableStrings.get(i) + ".dat"));
					//ConfigureVariables.tablefile.put(ConfigureVariables.tableStrings.get(i),br);
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				System.out.println("Exception");
				e2.printStackTrace();
			}
			
		}
		
		
		
		
		
		
			
		}
	
	
		
	}
	
	
	
	/*
	public static void NestedScan(ArrayList<String> tableStrings) {
		
		
			
		BufferedReader or = null ;
		try {
			or = new BufferedReader(new FileReader(ConfigureVariables.pathname + ConfigureVariables.tableStrings.get(0) + ConfigureVariables.FILEEXTENSION));
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
		
		
		String oline;
	    try {
			while ((oline = or.readLine()) != null) {
			    //System.out.println(oline);
				ot = Tuple.getTuple(oline, ConfigureVariables.createTableMap,ConfigureVariables.tableStrings.get(0).toLowerCase());
				//System.out.println(ot[0]);				
				//SelectionOperator.Selection(ot,ConfigureVariables.tableStrings.get(0).toLowerCase());
			    BufferedReader ir = null ;
				try {
					ir = new BufferedReader(new FileReader(ConfigureVariables.pathname + ConfigureVariables.tableStrings.get(1) + ConfigureVariables.FILEEXTENSION));
				} catch (FileNotFoundException e) {
					
					e.printStackTrace();
				}
			    String iline;
				while((iline = ir.readLine()) != null) {
				//System.out.println(iline);	
				it = Tuple.getTuple(iline, ConfigureVariables.createTableMap,ConfigureVariables.tableStrings.get(1).toLowerCase());
				//System.out.println(it[0]);	
				SelectionOperator.NSelection(ot,it,tableStrings.get(0));	
				}
			
			}
			ConfigureVariables.reset();
				
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception");
			e.printStackTrace();
		}
			
		
		
		
		
	}
	*/
	/*
	public void set(ArrayList<String> tableStrings) {
		int s = tableStrings.size();
		BufferedReader br[] = new BufferedReader[s];
		String lines[] = new String[s];
		FullScan(tableStrings, br,lines);
		
	}
	
	public  void FullScan(ArrayList<String> tableStrings, BufferedReader br[], String lines[]) {
		
		
		if(i<tableStrings.size()) {
		try {
			br[i] = new BufferedReader(new FileReader(ConfigureVariables.pathname + ConfigureVariables.tableStrings.get(i) + ConfigureVariables.FILEEXTENSION));
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
		try {
			while((lines[i] = br[i].readLine())!=null) {
				it = Tuple.getTuple(lines[i],ConfigureVariables.createTableMap,ConfigureVariables.tableStrings.get(i).toLowerCase());
				tuples.add(it);
				i++;
				FullScan(tableStrings, br,lines);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		
		
		
		
		
		
		
	}
	*/
	}
