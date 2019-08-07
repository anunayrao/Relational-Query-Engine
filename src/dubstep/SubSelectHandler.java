package dubstep;
//@Author - Apoorva Biseria
import java.util.List;



import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SubSelectHandler {
	public static void excecute(PlainSelect plain) {
		//List<SelectItem> subSelectItems = plain.getSelectItems();
	
		PlainSelect ps =(PlainSelect)((SubSelect) plain.getFromItem()).getSelectBody();
		//System.out.println(ps);
		FromItem subFromItem = ps.getFromItem();
		if(subFromItem instanceof SubSelect)
		{if (ps.getWhere()!=null) {
			//System.out.println("java");
			Expression expression = ps.getWhere();
			ConfigureVariables.whereArrayList.add(expression);
			
		}
			excecute(ps);
		}
		//System.out.println(subFromItem);
			
		
		else {	
			Table nameTable= (Table)subFromItem;
			String tnameString = nameTable.getName();
			ConfigureVariables.tableStrings.add(tnameString);
		if(ps.getJoins()!=null) {
		List<Join> subJoins = ps.getJoins();
		
		
		
		if (subJoins.contains("JOIN")) {
		//System.out.println(joins.size());
			//Configuration.conditionsArrayList.add(true);
			ConfigureVariables.JOIN = 1;
			
		for(Join i:subJoins)
		{
			System.out.println(i.getRightItem());
			String rightString =i.getRightItem().toString();
			ConfigureVariables.tableStrings.add(rightString);
			if(i.getOnExpression()!=null) {
				Expression onExpression = i.getOnExpression();
				ConfigureVariables.OnList.add(onExpression);
				
			}
			
		}}else {
			ConfigureVariables.JOIN = -1;
		for(Join i:subJoins) {
			System.out.println(i);
			//Table nameTable =
			
			ConfigureVariables.tableStrings.add(i.toString());
		}
		}
		System.out.println(subJoins.size());
		System.out.println(subJoins);
		
		
		}
		//PlainSelect plainSelect = (PlainSelect)subSelect.getSelectBody()	;
		if (ps.getWhere()!=null) {
			Expression expression = ps.getWhere();
			ConfigureVariables.whereArrayList.add(expression);
			//System.out.println("java");
		}
		}
		//System.out.println(subFromItem);
		}}	
	//Configuration.coList.add(colNameStrings);
	//Configuration.coList.add(tableList);
	
	