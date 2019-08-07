package dubstep;
////@Author - Anunay Rao
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;


public class MyTable  {
	//private static final long serialVersionUID = 1L;
	String tableName;
	FileReader fr;
	BufferedReader br;
	 ArrayList<ColumnDefinition> tableColDef;
	 ArrayList<ColumnDefinition> colDef;
	ArrayList<String> columnNames;
	 ArrayList<ColDataType> columnDatatype;
	boolean data;
	ArrayList<String> tuples = new ArrayList<String>();
	public MyTable(String tableName, ArrayList<ColumnDefinition> tableColDef, boolean data) {
		this.tableName = tableName;
		this.fr=null;
		this.br=null;
		this.tableColDef = tableColDef;
		this.data = data;
		
	}
	
	public void allocateReader() {
		try {
			if(data) {
			this.fr = new FileReader(ConfigureVariables.pathname + this.tableName + ConfigureVariables.FILEEXTENSION);
			}
			else {
				this.fr = new FileReader(ConfigureVariables.temppath + this.tableName + ConfigureVariables.FILEEXTENSION);
			}
			if(this.tableName.contains("|"))
			ConfigureVariables.files.add(this.tableName + ConfigureVariables.FILEEXTENSION);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.br = new BufferedReader(fr);
	}
	
	public String readtuple() {
		if(this.fr==null && this.br==null)
			this.allocateReader();
		try {
			String scan = br.readLine();
			if(scan==null) {
				br.close();
				this.br=null;
				this.fr=null;
				
			}
			return scan;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return tableName;
		
	}
}
