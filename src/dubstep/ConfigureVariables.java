package dubstep;
//@author - Anunay Rao

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ConfigureVariables {

	  public static String PROMPT = "$> ";
	  
	
	 public static String FILEEXTENSION= ".csv";
	 public static String pathname = "data/";
	// public static String temppath = "/Users/anunay/eclipse-workspace/Checkpoint0/src/dubstep/parsers/tempfolder/";
	public static String temppath = "tempfolder/"; 
	//public static String FILEEXTENSION= ".dat";
	//public static String pathname = "/Users/anunay/eclipse-workspace/Checkpoint0/src/dubstep/parsers/";
	  //CREATE TABLE LINEITEM(ORDERKEY INT,PARTKEY INT,SUPPKEY INT,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1),LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10),COMMENT VARCHAR(44),PRIMARY KEY (ORDERKEY,LINENUMBER));
//CREATE TABLE ORDERS(ORDERKEY INT,CUSTKEY INT,ORDERSTATUS CHAR(1),TOTALPRICE DECIMAL,ORDERDATE DATE,ORDERPRIORITY CHAR(15),CLERK CHAR(15),SHIPPRIORITY INT,COMMENT VARCHAR(79),PRIMARY KEY (ORDERKEY));	  
//CREATE TABLE CUSTOMER(CUSTKEY INT,NAME VARCHAR(25),ADDRESS VARCHAR(40),NATIONKEY INT,PHONE CHAR(15),ACCTBAL DECIMAL,MKTSEGMENT CHAR(10),COMMENT VARCHAR(117),PRIMARY KEY (CUSTKEY));

public static final String DELIMITER = "|";
	  public static List<String> colNameStrings = new ArrayList<String>();
	  public static HashMap <String,String>  aliases = new HashMap<String, String>();
	  public static ArrayList<Expression> whereArrayList =new ArrayList<Expression>();
	  public static ArrayList<String> tableStrings = new ArrayList<String>();
	  public static int JOIN = 0;
	  public static ArrayList<Expression> OnList = new ArrayList<Expression>();
	  public static HashMap<String,String> stats = new HashMap<String, String>();
	  public static boolean istpch1 = false;
	  public static boolean istpch3 = false;
	  public static boolean istpch5 = false;
	  public static boolean istpch6 =false;
	  public static boolean istpch12 = false;
	  public static String deleterow= null;
	  //index is used to Create indexes and store create table statements on disk.
	  public static boolean index = false;
	  public static boolean indextpch1makessense= false;
	  
	  
	  //public static HashMap<String,Integer> statsnum = new HashMap<String, Integer>();

	  //Table Name <Column Name, ColumnInfo>
	  //static HashMap<String, HashMap<String, ColumnInfo>> dbMap = new HashMap<String, HashMap<String,ColumnInfo>>();
	  //Table Name and Create Table Statement.
	  static HashMap<String, CreateTable> createTableMap = new HashMap<>();
	  static HashMap<String, BufferedReader> tablefile = new HashMap<String,BufferedReader>();
	  static HashMap<String, ArrayList<String>> tablecol = new HashMap<String,ArrayList<String>>();
	  static boolean allcol = false;
	  public static HashMap<String,String> tablealias = new HashMap<String, String>();
	  public static ArrayList<Object> colExp = new ArrayList<Object>(); 
	  public static List<SelectItem> testselect = new ArrayList<SelectItem>();
	public static boolean joinsget = false;
	 public static HashMap<String,String> aliastotable = new HashMap<String, String>();
	 public static ArrayList<String> talias = new ArrayList<String>();
	 public static ArrayList<OrderByElement> orderby = new ArrayList<OrderByElement>();
	 public static boolean firsttuple = true;
	 public static ArrayList<String> res = new ArrayList();
	 public static boolean isagg=false;
	 public static int count=0;
	 public static ArrayList<String> joinTables = new ArrayList();
	 // Dividing whereArrayList into joinCondition and nonJoinCondition
	 public static HashSet<Expression> joinCondition = new HashSet();
	 public static HashSet<Expression> nonJoinCondition = new HashSet();
	 public static HashMap<String,MyTable>  tableObjects = new HashMap();
	 public static HashMap<String,ArrayList<Expression>> tableCondition = new HashMap();
	 public static HashMap<String,String> colalias = new HashMap<>();
	 public static boolean inmem = false;
	 public static boolean ondisk = false;
	 public static long limit =0;
	 public static boolean descending  = false;
	 public static int countcheck=0;
	 //public static ArrayList<String> med = new ArrayList<>();
	 public static HashMap<String,ArrayList<String>> computedGroupBy = new HashMap<>();
		 public static ArrayList<Object> groupbylist =new ArrayList<>();
		public static boolean groupByPresent=false;
		public static Boolean desc= false;
		public static ArrayList<ColumnDefinition> groupbycd = new ArrayList<>();
	 //To clear joinCondition, nonJoinCondition, whereArrayList
		public static boolean beforejoin = true;
	public static ArrayList<String> afterjoincol = new ArrayList();
	public static ArrayList<String> files = new ArrayList();
	public static long starttime = 0L;
	public static boolean firstjoin = false;
	public static HashMap<String,ArrayList<Object>> computedGroupByo = new HashMap<>();
	public static boolean delete=false;
	public static ArrayList<String> afterDelete = new ArrayList<String>();
	public static HashMap<String, ArrayList<String>> mapinsert = new HashMap<String, ArrayList<String>>();
	public static HashMap<String, ArrayList<String>> mapdelete = new HashMap<String, ArrayList<String>>();
	// public static ArrayList<Object> computedGroupByO = new 
	 
	 
	 
/*

public static void main(String arg[]) {
	
	System.out.println(DELIMITER);

}
*/
@SuppressWarnings("unused")
public static void deletereset() {
	afterDelete.clear();
}
public static void reset() {
	
	delete = false;
	//afterDelete.clear();
	//ArrayList<Expression> whereArrayList, ArrayList<String> tableStrings,List<String> colNameStrings	
	istpch1=false;
	istpch3 = false;
	istpch5=false;
	istpch6 =false;
	istpch12 = false;
	starttime = 0L;
	res.clear();
	countcheck=0;
	descending = false;
		firstjoin = false;
		count=0;
		isagg=false;
		firsttuple = true;
		whereArrayList.clear();
		tableStrings.clear();
		colNameStrings.clear();
		allcol = false ;
		tablealias.clear();
		colExp.clear();
		testselect.clear();
		joinsget = false;
		aliastotable.clear();
		talias.clear();
		orderby.clear();
		afterjoincol.clear();
		beforejoin=true;
		files.clear();
		groupbycd.clear();
		limit = 0;
		joinTables.clear();
		desc=false;
		groupByPresent = false;
		groupbylist.clear();
		computedGroupBy.clear();
		colalias.clear();
		tableCondition.clear();
		joinCondition.clear();
		nonJoinCondition.clear();
		computedGroupByo.clear();
	  //System.out.println(whereArrayList);
	  //System.out.println(tableStrings);
	  //System.out.println(colNameStrings);
}

}