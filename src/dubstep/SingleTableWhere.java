package dubstep;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class SingleTableWhere {
	
	
	
	
	public static MyTable FilterWhere(MyTable table) {
		ArrayList<String> rows = new ArrayList();
	MyTable ftable = new MyTable (table.tableName+"|",table.tableColDef,false);
	ConfigureVariables.files.add(ftable.tableName+".csv");
	ArrayList<String> cn = new ArrayList<String>();
    ArrayList<ColDataType> cd = new ArrayList<ColDataType>();
    
       for(int i=0;i<table.columnNames.size();i++) {
       	cn.add(table.columnNames.get(i));
       	cd.add(table.columnDatatype.get(i));
       }
       ftable.columnDatatype = cd;
       ftable.columnNames =cn;
	String row=null;
	while((row=table.readtuple())!=null) {
	
		if(checkSelection(row, table.tableColDef, table.tableName))	{
			//System.out.println("yes");
			//rows.add(row);
			writeresult(row, ftable);
		}
	
	}
	//System.out.println("Write this"+rows);
	//for(int i=0;i<rows.size();i++) {
	//	System.out.println(rows.get(i));
	//}
	//System.out.println(rows.size()+"SIZE!!!!!!1");
	
		
	
		return ftable;
	}





public static boolean checkSelection(String str, ArrayList<ColumnDefinition> columnDefinitionList, String tableName) {
	//System.out.println("Check sel");
	PrimitiveValue t[];
	t = Tuple.getTupleVal(str, columnDefinitionList);
	
	Evaluator eval = new Evaluator(t);
	ArrayList<Expression> tc = ConfigureVariables.tableCondition.get(tableName);
	//System.out.println("Conditions::"+tc);
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

}