package dubstep;
//@Author - Anunay Rao
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class Evaluator extends Eval {
	
	//private HashMap<String, HashMap<String, ColumnInfo>> dbMap;
	public PrimitiveValue[] tuple;
	public Evaluator(PrimitiveValue[] tuple){
        //this.schema = schema;
        this.tuple = tuple;
        //this.dbMap = dbMap;
    }
	@Override
	public PrimitiveValue eval(Column c) throws SQLException {
		if(ConfigureVariables.beforejoin) {
		String tableName="";
		String colName = c.getColumnName().toLowerCase();
		//System.out.println(colName);
        int b = 0;
		if(c.getTable().getName()==null) {
			for(String key : ConfigureVariables.tablecol.keySet()) {
				for(String q : ConfigureVariables.tablecol.get(key)) {
					if(q.equals(colName.toLowerCase())) {
						 tableName = key;
						//System.out.println("NAME:"+tableName);
						b = 1;
						break;
					}
				}
				if(b==1)
					break;
			}
		}
		else {
		 tableName = c.getTable().getName();
		 //System.out.println(tableName);
		 /*
		 if(ConfigureVariables.tablealias.containsKey(tableName.toUpperCase())) {
			 tableName = ConfigureVariables.tablealias.get(tableName).toLowerCase();
		 }*/
		 //int wt = 1;
		 for(String s : ConfigureVariables.tablealias.keySet()) {
			 
			 if(tableName.equals(ConfigureVariables.tablealias.get(s))) {
				 tableName = s.toLowerCase();
				
				 //System.out.println("Here"+tableName);
				 break;
			 }
			 
			 
		 }
		}
        //System.out.println(colName);
        //System.out.println(tableName);
        
		
        int index = -1;
        
        /*
        if(ConfigureVariables.dbMap.containsKey(tableName)) {
        	HashMap<String, ColumnInfo> key = ConfigureVariables.dbMap.get(tableName);
        	int i = 0;
        	for (Map.Entry<String, ColumnInfo> entry1 : key.entrySet())
			{
				String s1 = entry1.getKey().toLowerCase();
				System.out.println(s1);
				//ColumnInfo c1 = entry1.getValue();
				if(s1.equals(colName)){
					index=i;
					break;
				}
				i++;
				//System.out.println(s+":"+s1 +" : "+ c1.colNo +":"+c1.colDataType );
			}
        	
        	
        }
        //////////////
        if(ConfigureVariables.tablecol.containsKey(tableName)) {
        	//HashMap<String, String> key = ConfigureVariables.tablecol.get(tableName);
        	int i = 0;
        	for (Map.Entry<String, String> entry1 : ConfigureVariables.tablecol.entrySet())
			{
				String s1 = entry1.getValue().toLowerCase();
				
				System.out.println(s1);
				//ColumnInfo c1 = entry1.getValue();
				if(s1.equals(colName)){
					index=i;
					//break;
				}
				i++;
				//System.out.println(s+":"+s1 +" : "+ c1.colNo +":"+c1.colDataType );
			}
        	
        	
        }
        */
        int i =0;
       //System.out.println(tableName);
       //System.out.println(colName);
        if(ConfigureVariables.delete) {
        	tableName = tableName.toLowerCase(); // Since not handling any aliases for now for Delete
        }
        
		if(ConfigureVariables.tablecol.containsKey(tableName)) {
        	ArrayList<String>  str = ConfigureVariables.tablecol.get(tableName);
        	for ( int a =0; a<str.size();a++) {
        		if((str.get(a)).equals(colName)) {
        			//System.out.println("index"+":"+colName+":"+i);
        			//ConfigureVariables.countcheck = ConfigureVariables.countcheck +1;
        			index = i;
        			break;
        		}
        		i++;
        	}
        }
        
        
        //System.out.println("INDEX:"+index);
        //System.out.println(tuple[0]);
		//System.out.println("evaluator"+index);
        return tuple[index];

	}
		else {
			String col = c.toString();
			//System.out.println(col);
			for(int i =0; i<ConfigureVariables.afterjoincol.size();i++) {
				
				if(col.equals(ConfigureVariables.afterjoincol.get(i))) {
					//System.out.println(i);
					/*
					if(col.equalsIgnoreCase("LINEITEM.EXTENDEDPRICE")) {
						System.out.println("LINEITEM.EXTENDEDPRICE::"+ i);
					}
					
					if(col.equalsIgnoreCase("LINEITEM.DISCOUNT")) {
						System.out.println("LINEITEM.DISCOUNT::"+i);
					}*/
					return tuple[i];
				}
			}
		}
		return null;
	
}
}









