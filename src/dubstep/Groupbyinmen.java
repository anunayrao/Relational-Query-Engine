package dubstep;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Groupbyinmen {
        public static void groupby(MyTable table, List groupbyElementsList) throws SQLException, IOException {
                ArrayList<String> colNames = new ArrayList<>();
                String row;
                ArrayList<String> ColumnNames = table.columnNames;
                ArrayList<ColDataType> datatypes =table.columnDatatype;
                ArrayList<ColumnDefinition> columns = table.tableColDef;
                ArrayList<Integer> indexes = new ArrayList<>();
                ArrayList<Integer> indexes2 = new ArrayList<>();

                ArrayList<String> selectindexes =new ArrayList<>();

                ArrayList<ColumnDefinition> columns2 = new ArrayList<ColumnDefinition>();
                int kt=0 ;
                //System.out.println("COLUMNNAMES:::"+ColumnNames);
                for(int i =0;i<ColumnNames.size();i++)
                {


                        ColumnDefinition col = new ColumnDefinition();
                        col.setColumnName(ColumnNames.get(i));
                        col.setColDataType(datatypes.get(i));
                        columns2.add(col);
                }

                //ArrayList<MyTable> tables = new ArrayList();
                //HashMap<Integer,MyTable> tableinfo =  new HashMap();
        /*      SelectItem aggo=null;
                int i =0;
                int fi = 0;
                for(SelectItem si : ConfigureVariables.testselect) {

                        if(si.toString().contains("SUM")) {
                                aggo = si;
                                fi =i;
                                //System.out.println(fi);

                        }
                        i++;
                                }

                */


                /*      

                for ( i=0;i<columns.size();i++) {
                         String colName = columns.get(i).getColumnName().toLowerCase();
                         ColumnNames.add(colName);
                 ColDataType colDataType = columns.get(i).getColDataType();
                 datatypes.add(colDataType);
                }

                */

                //indexes of groupby columns
                int g = 0;
                for(Object o : groupbyElementsList) {
                                        int l = 0;
                                for(String s : ColumnNames) {
                                                if(o.toString().toLowerCase().equals(s.toLowerCase())) {
                                                        indexes.add(l);
                                                }
                                                l++;
                                        }

                }       
                /*
                for(SelectItem s:ConfigureVariables.testselect) {
                        System.out.println("Select");
                        System.out.println(s.toString());
                }*/

                for(SelectItem s:ConfigureVariables.testselect)
                {       
                        int l=0;
                        for(String s1 : ColumnNames) {

                                         if(s.toString().toLowerCase().equals(s1.toLowerCase())) {
                                                 {      indexes2.add(l);
                                        break;}

                                }
                                         else {
                                                 if(s.toString().toLowerCase().contains("sum")) {
                                                 if(s.toString().toLowerCase().contains(s1.toString().toLowerCase())) {
                                                         indexes2.add(l);
                                                         break;
                                                 }}
                                         }
                                l++;
                        }
                }
                for(Integer h:indexes2)
                {  
                        //System.out.println(h);

                 ConfigureVariables.groupbycd.add(columns.get(h));
                 //System.out.println( "Grouped by" + ConfigureVariables.groupbycd.get(g));
                 g++;
                }
                for(ColumnDefinition c:ConfigureVariables.groupbycd) {
                        for(SelectItem s:ConfigureVariables.testselect) {
                                if(s.toString().toLowerCase().contains(c.getColumnName().toLowerCase()))
                                {       
                                        String alias =((SelectExpressionItem) s).getAlias();;
                                        if(alias!=null) {
                                                {
                                                        //System.out.println(alias+ "=alias");
                                                c.setColumnName(alias);
                                                }
                                        }

                                }
                        }

                }
                //System.out.println(ConfigureVariables.groupbycd+"LAALALALLAL");
                //for( i= 0;i<indexes.size();i++)
                //System.out.println(indexes.get(i));
                //ArrayList<String > med = new ArrayList<>();
                //FileWriter fileWriter =new FileWriter(ConfigureVariables.temppath+"groupedfile"+ConfigureVariables.FILEEXTENSION);
                //BufferedWriter  bw = new BufferedWriter(fileWriter);

                //String lastKey = "";

                while((row = table.readtuple())!=null) {
                        String tempRow[] = row.split("\\|");
                        String finalKey="";
                        for( int i=0; i< indexes.size(); i++) {
                                String tempKey = tempRow[indexes.get(i)];
                                finalKey += tempKey + "|";

                        }
                        //System.out.println("Lastkey:"+"value"+lastKey);
                        //System.out.println("Finalkey:"+"value"+finalKey);
                        //if(finalKey.equals(lastKey)) {

                        //System.out.println("final");
                        int z=0;
                        ArrayList<String > med = new ArrayList<>();
                        for(int k =0;k<ConfigureVariables.testselect.size();k++)
                        {
                                med.add(null);
                        }
                        for(SelectItem si:ConfigureVariables.testselect) {
                        PrimitiveValue[] t = Tuple.getTupleVal(row, columns2);

                        //if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
                                //int k = ConfigureVariables.computedGroupBy.get(finalKey);

                                //aggregate for already containing key
                        if(!(si.toString().contains("SUM")||si.toString().contains("AVG")||si.toString().contains("COUNT"))) {
                                if(si instanceof SelectExpressionItem) {
                                        Evaluator eval = new Evaluator(t);
                                        try {
                                                PrimitiveValue result = eval.eval(((SelectExpressionItem) si).getExpression());
                                                //System.out.println(result.toRawString());
                                                if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
                                                        med.set(z,result.toRawString());
                                                        z++;

                                                }
                                                else  {
                                                        med.set(z,result.toRawString());

                                                         ConfigureVariables.computedGroupBy.put(finalKey, med);
                                                         z++;
                                                }


                                        } catch (SQLException e) {
                                                // TODO Auto-generated catch block
                                                e.printStackTrace();
                                        }

                        }}
                        else if(si.toString().contains("SUM")) {

                                        //ConfigureVariables.isagg = true;
                                        Expression exp = ((SelectExpressionItem) si).getExpression();
                                        Function funct = (Function)exp;
                                        Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);

                                        //System.out.println("Expression:"+exp1);
                                        Evaluator eval = new Evaluator(t);

                                                PrimitiveValue result = eval.eval(exp1);
                                                //System.out.println(result.toRawString());
                                                if(result instanceof DoubleValue) {
                                                        if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
                                                                ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
                                                                if(k.get(z)==null) {
                                                                        k.set(z, 0);
                                                                }
                                                                Double sum = Double.parseDouble(k.get(z).toString()) + ((DoubleValue) result).getValue();
                                                                med.set(z, sum.toString());
                                                                ConfigureVariables.computedGroupBy.put(finalKey,med);
                                                                z++;

                                                        }
                                                        else  {

                                                                med.set(z, result.toString());
                                                                 ConfigureVariables.computedGroupBy.put(finalKey, med);
                                                                 z++;
                                                        }

                                                }
                                                else if(result instanceof LongValue) {
                                                        if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
                                                                ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);

                                                                if(k1.get(z)==null) {
                                                                        k1.set(z, 0);
                                                                }
                                                                Long sum = Long.parseLong(k1.get(z).toString()) + ((LongValue) result).getValue();
                                                                med.set(z, sum.toString());
                                                                ConfigureVariables.computedGroupBy.put(finalKey, med);
                                                                z++;

                                                        }
                                                        else  {
                                                                med.set(z, result.toRawString());
                                                                 ConfigureVariables.computedGroupBy.put(finalKey,med);
                                                                 z++;
                                                        }
                                                }
                                        }
                        else if(si.toString().contains("AVG")) {

                                //ConfigureVariables.isagg = true;
                                Expression exp = ((SelectExpressionItem) si).getExpression();
                                Function funct = (Function)exp;
                                Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);

                                //System.out.println("Expression:"+exp1);
                                Evaluator eval = new Evaluator(t);

                                        PrimitiveValue result = eval.eval(exp1);
                                        //System.out.println(result.toRawString());
                                        if(result instanceof DoubleValue) {
                                                if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
                                                        ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
                                                        if(k.get(z)==null) {
                                                                k.set(z, 0);
                                                        }
                                                        Double t0 = Double.parseDouble(k.get(z).toString())*kt;
                                                        Double sum = t0 + ((DoubleValue) result).getValue();
                                                        kt++;
                                                        Double avg= (double) (sum/kt);
                                                        med.set(z, sum.toString());
                                                        ConfigureVariables.computedGroupBy.put(finalKey,med);
                                                        z++;

                                                }
                                                else  {
                                                        kt++;
                                                        Double avg=((DoubleValue) result).getValue()/kt;
                                                        med.set(z, result.toString());
                                                         ConfigureVariables.computedGroupBy.put(finalKey, med);
                                                         z++;
                                                }

                                        }
                                        else if(result instanceof LongValue) {
                                                if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
                                                        ArrayList k1 = ConfigureVariables.computedGroupBy.get(finalKey);

                                                        if(k1.get(z)==null) {
                                                                k1.set(z, 0);
                                                        }

                                                        Long t0 = Long.parseLong(k1.get(z).toString())*kt;
                                                        Long sum = t0 + ((LongValue) result).getValue();
                                                        kt++;
                                                        Double avg= (double) (sum/kt);
                                                        //med.set(z, sum.toString());
                                                        med.set(z, avg.toString());
                                                        ConfigureVariables.computedGroupBy.put(finalKey, med);
                                                        z++;

                                                }
                                                else  {
                                                        kt++;
                                                        Double avg=((DoubleValue) result).getValue()/kt;
                                                        med.set(z, avg.toString());
                                                         ConfigureVariables.computedGroupBy.put(finalKey,med);
                                                         z++;
                                                }
                                        }
                                }
                        else if(si.toString().contains("COUNT")) {

                                //ConfigureVariables.isagg = true;
                                Expression exp = ((SelectExpressionItem) si).getExpression();
                                Function funct = (Function)exp;
                                Expression exp1 =  (Expression)funct.getParameters().getExpressions().get(0);

                                //System.out.println("Expression:"+exp1);
                                //Evaluator eval = new Evaluator(t,ConfigureVariables.dbMap);

                                //      PrimitiveValue result = eval.eval(exp1);
                                        //System.out.println(result.toRawString());

                                                if(ConfigureVariables.computedGroupBy.containsKey(finalKey)) {
                                                        ArrayList k = ConfigureVariables.computedGroupBy.get(finalKey);
                                                        if(k.get(z)==null) {
                                                                k.set(z, 0);
                                                        }
                                                        Double sum = Double.parseDouble(k.get(z).toString()) + 1;
                                                        med.set(z, sum.toString());
                                                        ConfigureVariables.computedGroupBy.put(finalKey,med);
                                                        z++;

                                                }
                                                else  {

                                                        med.set(z, "1");
                                                         ConfigureVariables.computedGroupBy.put(finalKey, med);
                                                         z++;
                                                }

                                }
                                //inside si
                        }
                        //lastKey = finalKey;
                        }



                }









}
