package dubstep;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

public class GroupBy {
	
	public static void getGroupBy(MyTable table, List groupbyElementsList) throws SQLException {
		ArrayList<String> colNames = new ArrayList<>();
		String row;
		ArrayList<String> ColumnNames =new ArrayList<>();
		ArrayList<ColDataType> datatypes =new ArrayList();
		ArrayList<ColumnDefinition> columns = table.tableColDef;
		ArrayList<Integer> indexes = new ArrayList<>();
		ArrayList<String> selectindexes =new ArrayList<>();
		//ArrayList<MyTable> tables = new ArrayList();
		//HashMap<Integer,MyTable> tableinfo =  new HashMap();
		SelectItem aggo=null;
		int i =0;
		int fi = 0;
		for(SelectItem si : ConfigureVariables.testselect) {
			
			if(si.toString().contains("SUM")) {
				aggo = si;
				fi =i;
				
			}
			i++;
				}
	for(SelectItem si:ConfigureVariables.testselect) {
		
			
		
		for ( i=0;i<columns.size();i++) {
			 String colName = columns.get(i).getColumnName().toLowerCase();
			 ColumnNames.add(colName);
	         ColDataType colDataType = columns.get(i).getColDataType();
	         datatypes.add(colDataType);
		}
		
		//indexes of groupby columns
		for(Object o : groupbyElementsList) {
					int l = 0;
				for(String s : ColumnNames) {
						if(o.toString().toLowerCase().contains(s.toLowerCase())) {
							indexes.add(l);
						}
						l++;
					}
				
		}	
		for( i= 0;i<indexes.size();i++)
		System.out.println(indexes.get(i));
		
		/*while((row = table.readtuple())!=null) {
			String tempRow[] = row.split("\\|");
			String finalKey="";
			for( i=0; i< indexes.size(); i++) {
				String tempKey = tempRow[indexes.get(i)];
				finalKey += tempKey + "|";
				
			}
			
			
			PrimitiveValue[] t = Tuple.getTupleVal(row, columns);
			
			//if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
				//int k = ConfigureVariables.computedGroupBy.get(finalKey);
				//aggregate for already containing key
				if(aggo.toString().contains("SUM")) {
					//ConfigureVariables.isagg = true;
					Expression exp = ((SelectExpressionItem) aggo).getExpression();
					Function funct = (Function)exp;
					Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);
					
					//System.out.println(exp1);
					Evaluator eval = new Evaluator(t,ConfigureVariables.dbMap);
					
						PrimitiveValue result = eval.eval(exp1);
						//System.out.println(result.toRawString());
						
						if(result instanceof DoubleValue) {
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								String k = ConfigureVariables.computedGroupBy.get(finalKey);
								
								Double sum = Double.parseDouble(k) + ((DoubleValue) result).getValue();
								ConfigureVariables.computedGroupBy.put(finalKey, sum.toString());
							
							}
							else  {
								 ConfigureVariables.computedGroupBy.put(finalKey, result.toRawString());
							}
							
						}
						else if(result instanceof LongValue) {
							if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
								String k1 = ConfigureVariables.computedGroupBy.get(finalKey);
								
								Long sum = Long.parseLong(k1) + ((LongValue) result).getValue();
								ConfigureVariables.computedGroupBy.put(finalKey, sum.toString());
							
							}
							else  {
								 ConfigureVariables.computedGroupBy.put(finalKey, result.toRawString());
							}
						}
					}
				}*/
		}
	}}

			

	

		

		
		
		
				
