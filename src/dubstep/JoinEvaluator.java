package dubstep;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class JoinEvaluator extends Eval {

	public PrimitiveValue[] otuple;
	public PrimitiveValue[] ituple;
	public JoinEvaluator(PrimitiveValue[] otuple,PrimitiveValue[] ituple){
        //this.schema = schema;
        this.ituple = ituple;
        this.otuple = otuple;
        //this.dbMap = dbMap;
    }
	@Override
	public PrimitiveValue eval(Column c) throws SQLException {
		String tableName="";
		String colName = c.getColumnName().toLowerCase();
		int wt = 0;
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
		 //System.out.println("HERE"+tableName);
		 /*
		 if(ConfigureVariables.tablealias.containsKey(tableName.toUpperCase())) {
			 tableName = ConfigureVariables.tablealias.get(tableName).toLowerCase();
		 }*/
		/*
		 System.out.println("HERE");
		 for(String k : ConfigureVariables.aliastotable.keySet()) {
			 System.out.println(k +":" + ConfigureVariables.aliastotable.get(k));
		 }
		 */
		 if(ConfigureVariables.aliastotable.containsKey(tableName.toUpperCase())) {
			 //tableName = ConfigureVariables.aliastotable.get(tableName.toUpperCase());
		 
		 for(String s : ConfigureVariables.aliastotable.keySet()) {
			 wt++;
			 if(tableName.equals(s)) {
				 
				 tableName = ConfigureVariables.aliastotable.get(s).toLowerCase();
				
				 //System.out.println("Here"+tableName);
				 break;
			 }
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
       //System.out.println("huurrr"+tableName);
       //System.out.println("huuuu"+colName);
        
		if(ConfigureVariables.tablecol.containsKey(tableName)) {
        	ArrayList<String>  str = ConfigureVariables.tablecol.get(tableName);
        	for ( int a =0; a<str.size();a++) {
        		if((str.get(a)).equals(colName)) {
        			index = i;
        			break;
        		}
        		i++;
        	}
        }
        
        
        //System.out.println("INDEX:"+index);
        //System.out.println(tuple[0]);
        if(wt==1)
		return otuple[index];
        
        else {
        	//System.out.println("INDEX: "+index);
        	return ituple[index];	
        }
        

	}

}
