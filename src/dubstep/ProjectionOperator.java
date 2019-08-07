package dubstep;
////@Author - Anunay Rao and Apoorva Biseria
import java.sql.SQLException;
////@Author - Anunay Rao
import java.util.ArrayList;


import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator {

	public static void Projection(String tableName, PrimitiveValue [] t) {
		PrimitiveValue tu[];
		//System.out.println(ConfigureVariables.colNameStrings);
		//System.out.println("Ptanle"+tableName);
		//System.out.println(ConfigureVariables.tablealias.get(tableName));
		/*
		if(!ConfigureVariables.colNameStrings.get(0).contains(".")) {
			for(int i = 0; i<ConfigureVariables.colNameStrings.size(); i++) {
				ConfigureVariables.colNameStrings.set(i,tableName.toUpperCase() + "."+ConfigureVariables.colNameStrings.get(i));
				//System.out.println(ConfigureVariables.tablealias.get(tableName.toUpperCase()));
			}
			
		}
		*/
		
		
		
		//&& 
		
		//System.out.println("AFter"+ConfigureVariables.colNameStrings);
		//CREATE TABLE PLAYERS(ID string, FIRSTNAME string, LASTNAME string, FIRSTSEASON int, LASTSEASON int, WEIGHT int, BIRTHDATE date);
		//SELECT P1.FIRSTNAME FROM PLAYERS P1 WHERE P1.WEIGHT>200;
		ArrayList<String>  str1 = ConfigureVariables.tablecol.get(tableName);
		//System.out.println("str1::::::"+str1);
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
				/*String s = si.toString();
				//if(si.getExpression().getName().equals("MIN"))
				
				String exp = s.substring(s.indexOf("(") +1, s.indexOf(")"));
				String sub[] = exp.split(" ");
				String finalexp="";
				for(int z=0; z<sub.length;z++) {
					//System.out.println(sub[z]);
					int q=0;
					for(String st : str1) {
						System.out.println("L:"+st);
						if((tableName+"."+st).toUpperCase().equals(sub[z])) {
							sub[z]= t[q].toRawString();
							break;
						}
						q++;
					}
				}
				for(int z=0; z<sub.length;z++) {
					System.out.println("AA:"+sub[z]);
				}
				
				ScriptEngineManager m = new ScriptEngineManager();
				ScriptEngine engine = m.getEngineByName("JavaScript");
				String f = "40 + 10";
				try {
					System.out.println(engine.eval(f));
				} catch (ScriptException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//CREATE TABLE Q(A STRING, B INT);
				//SELECT MIN(Q.A + Q.B) FROM Q;
				//SELECT MIN(Q.B), MAX(Q.B) FROM Q;
				//SELECT Q.A FROM Q,S WHERE Q.A=S.A AND Q.B=S.B AND Q.A>20 AND Q.B=10;
				 SELECT Q.A FROM Q WHERE Q.A>10;
				 SELECT Q.A, T.A FROM Q,T WHERE Q.B>20 AND T.C>1000;
				 //SELECT MIN(Q.B), MAX(Q.B), COUNT(*), COUNT(*) FROM Q;
				 */
				
				/*System.out.println(s);
				String e[] = s.split("(");
				System.out.println(e[1]);
				String f[] = e[1].split(")");
				System.out.println(f[0]);
				String exp = f[0];
				System.out.println(exp);*/
				
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
		
		/*
		
		
		
		if(!ConfigureVariables.allcol) {
			ArrayList<String>  str = ConfigureVariables.tablecol.get(tableName);
			for(int i = 0 ; i <ConfigureVariables.colExp.size(); i++) {
				Object o = ConfigureVariables.colExp.get(i);
				if(o instanceof Expression) {
					Evaluator eval = new Evaluator(t, ConfigureVariables.dbMap);
					try {
						PrimitiveValue result = eval.eval((Expression)o);
						if(i<ConfigureVariables.colExp.size()-1)
						System.out.print(result.toRawString()+"|");
						else
							System.out.print(result.toRawString());
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
			
			/////////////////////////////////////////////////
		/*	int k =1;
			//System.out.println(ConfigureVariables.colNameStrings);
		for(String s : ConfigureVariables.colNameStrings) {
			
        	ArrayList<String>  str = ConfigureVariables.tablecol.get(tableName);
        	//System.out.println("P1 aana : "+ConfigureVariables.tablealias.get(tableName.toUpperCase()));
        	
        	for ( int a =0; a<str.size();a++) {
        		//System.out.println(str.get(a));
        		
        		//System.out.println("S:"+s);
        		//Here -1
        		if(k==ConfigureVariables.colNameStrings.size()) {
        			if((ConfigureVariables.tablealias.get(tableName.toUpperCase())+"."+str.get(a).toUpperCase()).equals(s.toUpperCase())) {
            			
        				System.out.print(t[a].toRawString());
            			break;
        		}
        		}
        			else {
        		if((ConfigureVariables.tablealias.get(tableName.toUpperCase())+"."+str.get(a).toUpperCase()).equals(s.toUpperCase())) {
        			System.out.print(t[a].toRawString()+"|");
        			break;
        		}
        			}
        		
        	}
        	k++;
       
	}*/
		////////////////////////////////////////////////
		
		
		
		
		/*
		 
		 
		 
		 
		System.out.println();
	}
		else if(ConfigureVariables.allcol) {
			ArrayList<String>  str = ConfigureVariables.tablecol.get(tableName);
        	for ( int a =0; a<str.size();a++) {
        		//System.out.println(str.get(a));
        		//System.out.println("S:"+s);
        			if(a==str.size()-1) {
        				System.out.print(t[a].toRawString());
        				
        			}
        			else {
        			System.out.print(t[a].toRawString()+"|");
        			
        			}
        		
        	}
        	System.out.println();
        	
        	
		}
		*/
	}
	
	public static void NProjection(String tableName, PrimitiveValue []otuple, PrimitiveValue []ituple) {
		//System.out.println(tableName);
		//System.out.println(ConfigureVariables.tablecol.get(tableName.toLowerCase()).get(0));
		//System.out.println(ConfigureVariables.talias);
		
		ArrayList<String>  str1 = ConfigureVariables.tablecol.get(tableName.toLowerCase());
		int i =0;
		for(SelectItem si : ConfigureVariables.testselect) {
			
			if(!ConfigureVariables.talias.isEmpty()) {
			if((si.toString().equals(ConfigureVariables.talias.get(0).toUpperCase()+".*"))   ){
				for ( int a =0; a<str1.size();a++) {
	        		//System.out.println(str.get(a));
	        		//System.out.println("S:"+s);
	        			if((a==str1.size()-1)&&(i == ConfigureVariables.testselect.size()-1)) {
	        				System.out.print(otuple[a].toRawString());
	        				
	        			}
	        			else {
	        			System.out.print(otuple[a].toRawString()+"|");
	        			
	        			}
	        		
	        	}
				i++;
			}
			else if((si.toString().equals(ConfigureVariables.talias.get(1).toUpperCase()+".*"))) {
				for ( int a =0; a<str1.size();a++) {
	        		//System.out.println(str.get(a));
	        		//System.out.println("S:"+s);
	        			if((a==str1.size()-1)&&(i == ConfigureVariables.testselect.size()-1)) {
	        				System.out.print(ituple[a].toRawString());
	        				
	        			}
	        			else {
	        			System.out.print(ituple[a].toRawString()+"|");
	        			
	        			}
	        		
	        	}
				i++;
			}}
			else if(si instanceof SelectExpressionItem) {
				JoinEvaluator eval = new JoinEvaluator(otuple,ituple);
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
/*		
for(SelectItem si : ConfigureVariables.testselect) {
			
			if(si instanceof AllColumns || (si.toString().equals(ConfigureVariables.tablealias.get(tableName.toUpperCase())+".*")) ){
				for ( int a =0; a<str1.size();a++) {
	        		//System.out.println(str.get(a));
	        		//System.out.println("S:"+s);
	        			if((a==str1.size()-1)&&(i == ConfigureVariables.testselect.size()-1)) {
	        				System.out.print(ituple[a].toRawString());
	        				
	        			}
	        			else {
	        			System.out.print(ituple[a].toRawString()+"|");
	        			
	        			}
	        		
	        	}
				i++;
			}
			else if(si instanceof SelectExpressionItem) {
				JoinEvaluator eval = new JoinEvaluator(otuple,ituple, ConfigureVariables.dbMap);
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
		*/
	
		System.out.println();
		
	}
	
}
	
	

