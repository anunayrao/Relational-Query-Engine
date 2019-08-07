package dubstep;

import java.util.HashMap;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class PlainStatementExecutor {
	
	PlainSelect plainstatement;
	HashMap createTableMap;
	
	public PlainStatementExecutor(PlainSelect plainstatement, HashMap createTableMap ) {
		this.plainstatement = plainstatement;
		this.createTableMap = createTableMap;
		
	}

}

