import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.util.Map;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.io.LineNumberReader;
import java.sql.SQLException;

public class QueryGenerator
{
	public static List<String> S = new ArrayList<String>();//Selection Atribute
	public static int N = 0;//Number of grouping variables
	public static List<String> V = new ArrayList<String>();//Set of grouping attribute
	public static List<List> F = new ArrayList<List>();//Aggregate tuple
	public static List<String> SIG = new ArrayList<String>();//selection condition
	public static String G = "";//having condition
	public static String usr = "postgres";//postgres user name
	public static String pass = "qwerty";//postgres password
	public static String url = "jdbc:postgresql://localhost:5432/CS562";//postgres database URL
	public static String tableName = "sales";//table name
	public static List<String> tableStruct = new ArrayList<String>();//mAttribute or column name name of table
	public static Map<String, String> table = new HashMap<>();// hash map for the table
	
	
	public static List<String> readByLine(String fileName) //function to read input file line by line
	{
		List<String> lines = new ArrayList<String>();
		String line = null;
		LineNumberReader reader = null;
		try 
		{
            reader = new LineNumberReader(new FileReader(fileName));
            while ((line = reader.readLine()) != null) 
			{
                lines.add(line);
            }
		}
		catch(FileNotFoundException e) 
		{
            e.printStackTrace();
        }
		catch(IOException e) {
            e.printStackTrace();
        }
        finally 
		{
			try 
			{
                if(reader != null) 
				{
                    reader.close();
                }
            }
            catch(IOException e) 
			{
                e.printStackTrace();
            }
        }
		return lines;
    }
    
	
	public static void main(String[] args) //main program
	{
		int i = 0;
		try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Database Successfully Loaded.");
        }
        catch(Exception e) {
            System.out.println("Database Loading FAILED.");
            e.printStackTrace();
        }
        
        try {
            Connection conn = DriverManager.getConnection(url, usr, pass);
            System.out.println("Database Connection SUCCESSFUL.\n");

            Statement stat = conn.createStatement();

            // find out attribute data type for the given table from information schema
            ResultSet rs = stat.executeQuery("SELECT * FROM information_schema.columns WHERE table_name = '"+tableName+"'");
			while (rs.next()) 
			{
                String column_name = rs.getString("column_name");
                String data_type = rs.getString("data_type");
                tableStruct.add(column_name);
                String modSTR = InputModifier.DataType(data_type);
                table.put(column_name, modSTR);
				
            }
		}
		catch(SQLException e) {
            System.out.println("Cannot featch table data");
            e.printStackTrace();
        }
		List<String> lines = readByLine("input");
		//reading input file and initializing 6 attribute of Phi
		for(i = 0; i < lines.size(); i++) 
		{
			if(lines.get(i).indexOf("SELECT ATTRIBUTE") != -1) 
			{
				String[] str = lines.get(i+1).replaceAll(" ", "").split(",");
                for(String s : str) 
				{
					S.add(s);
				}
			}
            else if(lines.get(i).indexOf("NUMBER OF GROUPING VARIABLES") != -1) 
			{
                N = Integer.parseInt(lines.get(i+1));
            }
            else if(lines.get(i).indexOf("GROUPING ATTRIBUTES") != -1) {
                String[] str = lines.get(i+1).replaceAll(" ", "").split(",");
                for(String s : str) {
                    V.add(s);
                }
            }
            else if(lines.get(i).indexOf("F-VECT") != -1) 
			{
                String[] str = lines.get(i+1).replaceAll(" ", "").split(";");
                for(String S : str) {
                    List<String> list = new ArrayList<String>();
                    if((S.equals(""))) {
                        list.add(S);
                        F.add(list);
                    }
                    else {
                        String[] str2 = S.split(",");
                        for(String s : str2) 
						{
                            String _type = null;
                            String[] _str = s.split("_");
                            // aggregate functions
                            if(_str[0].equals("avg")) 
							{
                                // sum
                                String _sum = "sum_"+_str[1]+"_"+_str[2];
                                _type = InputModifier.AggregateType(_sum);
                                table.put(_sum, _type);
                                list.add(_sum);
                                // count
                                String _count = "count_"+_str[1]+"_"+_str[2];
                                table.put(_count, "int");
                                list.add(_count);
                            }
                            else {
                                list.add(s);
                            }
                            _type = InputModifier.AggregateType(s);
                            table.put(s, _type);
                        }
                        // remove duplicates
                        List<String> list2 = new ArrayList<String>();
                        for (String dup : list) {
                            if (Collections.frequency(list2, dup) < 1) {
                                list2.add(dup);
                            }
                        }
                        F.add(list2);
                    }
                }
            }
            // SELECT CONDITION-VECT
            else if(lines.get(i).indexOf("SELECT CONDITION-VECT") != -1) {
                String[] str = lines.get(i+1).split(";");
                for(int j = 0; j < str.length; j++) {
                    SIG.add(InputModifier.SigmaCondition(str[j], j));
                }
            }
            // HAVING Clause
            else if(lines.get(i).indexOf("HAVING CONDITION") != -1) {
                G = InputModifier.HavingCondition(lines.get(i+1));
            }
            else { /* do nothing */ }
        }
		FileWriter writer = null;//to new code for the specific query
		try 
		{
		    File file = new File("GeneratedQuery.java");//file name
		    if(file.createNewFile()) 
			{
		        System.out.println("File Name: --GeneratedQuery.java-- created successfully.");
			}
			else 
			{
		        System.out.println("File already exists.");
		    }
			writer = new FileWriter("GeneratedQuery.java", true);
			writer.write("import java.sql.*;"+"\n");
		    writer.write("import java.util.*;"+"\n\n");
			writer.write("@SuppressWarnings(\"unchecked\")"+"\n");
		    writer.write("public class GeneratedQuery \n{\n");
            writer.write("\tpublic static Map<String, List> mfStruct = new HashMap<>();"+"\n");
            writer.write("\tpublic static ResultSet rs = null;"+"\n");
            writer.write("\tpublic static void main(String[] args) \n\t{\n");
            writer.write("\t\tString usr =\""+usr+"\";"+"\n");
            writer.write("\t\tString pass =\""+pass+"\";"+"\n");
            writer.write("\t\tString url =\""+url+"\";"+"\n");
            writer.write("\t\t// define MF-Struct"+"\n");
			for(i = 0; i < V.size(); i++) 
			{
                String type = table.get(V.get(i));
                if(type.equals("int")) 
				{
					type = "Integer"; 
				}
                writer.write("\t\tmfStruct.put(\""+V.get(i)+"\", new ArrayList<"+type+">());"+"\n");
				
            }
			for(i = 0; i < F.size(); i++) 
			{
                if(F.get(i).get(0).equals("")) 
				{
                    continue;
                }
                for(int j = 0; j < F.get(i).size(); j++) 
				{
                    String type = table.get(F.get(i).get(j));
                    if(type.equals("int")) 
					{
						type = "Integer";
					}
                    writer.write("\t\tmfStruct.put(\""+F.get(i).get(j)+"\", new ArrayList<"+type+">());"+"\n");
                }
            }
            writer.write("\t\t//DATABASE Connection Set up"+"\n");
            writer.write("\t\ttry \n\t\t{"+"\n");
            writer.write("\t\t\tClass.forName(\"org.postgresql.Driver\");"+"\n");
            writer.write("\t\t\tSystem.out.println(\"Database Successfully Loaded.\");"+"\n");
            writer.write("\t\t}"+"\n");
            writer.write("\t\tcatch(Exception e) \n\t\t{"+"\n");
            writer.write("\t\t\tSystem.out.println(\"Database Loading FAILED.\");"+"\n");
            writer.write("\t\t\te.printStackTrace();"+"\n");
            writer.write("\t\t}"+"\n\n");
            writer.write("\t\ttry \n\t\t{"+"\n");
            writer.write("\t\t\tConnection conn = DriverManager.getConnection(url, usr, pass);"+"\n");
            writer.write("\t\t\tSystem.out.println(\"Database Connection SUCCESSFUL.\\n\");"+"\n\n");
            writer.write("\t\t\tStatement stat = conn.createStatement();"+"\n\n");
            writer.write("\t\t\trs = stat.executeQuery(\"SELECT * FROM sales\");"+"\n");
            writer.write("\t\t\twhile (rs.next()) \n\t\t\t{"+"\n\n");
			for(i = 0; i < V.size(); i++) 
			{
                String v = V.get(i);
                writer.write("\t\t\t\tString "+v+" = rs.getString(\""+v+"\");"+"\n");
            }
			writer.write("\t\t\t\tboolean flag = true;"+"\n");
            writer.write("\t\t\t\tfor(int i = 0; i < mfStruct.get(\""+V.get(0)+"\").size(); i++)\n\t\t\t\t{\n");
            String ifF0 = V.get(0)+".equals(("+table.get(V.get(0))+")mfStruct.get(\""+V.get(0)+"\").get(i)+\"\")";
            for(i = 1; i < V.size(); i++) {
                ifF0 += "&&"+V.get(i)+".equals(("+table.get(V.get(i))+")mfStruct.get(\""+V.get(i)+"\").get(i)+\"\")";
            }
            writer.write("\t\t\t\t\tif("+ifF0+") \n\t\t\t\t\t{"+"\n");
            writer.write("\t\t\t\t\t\tflag = false;"+"\n");
            writer.write("\t\t\t\t\t\tbreak;"+"\n");
            writer.write("\t\t\t\t\t}"+"\n");
            writer.write("\t\t\t\t}"+"\n");
            writer.write("\t\t\t\t// add a new combination of grouping attributes"+"\n");
            writer.write("\t\t\t\tif(flag) \n\t\t\t\t{"+"\n");
			for(i = 0; i < V.size(); i++) 
			{
                String type = table.get(V.get(i));
                if(type.equals("int")) {
                    writer.write("\t\t\t\t\tmfStruct.get(\""+V.get(i)+"\").add(Integer.parseInt("+V.get(i)+"+\"\"));"+"\n");
                }
                else if(type.equals("Double")) {
                    writer.write("\t\t\t\t\tmfStruct.get(\""+V.get(i)+"\").add(Double.parseDouble("+V.get(i)+"+\"\"));"+"\n");
                }
                else {
                    writer.write("\t\t\t\t\tmfStruct.get(\""+V.get(i)+"\").add("+V.get(i)+"+\"\");"+"\n");
                }
            }
			for(i = 0; i < F.size(); i++) 
			{
                if(F.get(i).get(0).equals("")) {
                    continue;
                }
                for(int j = 0; j < F.get(i).size(); j++) {
                    writer.write("\t\t\t\t\tmfStruct.get(\""+F.get(i).get(j)+"\").add(null);\n");
                }
            }
			writer.write("\t\t\t\t}"+"\n");
            writer.write("\t\t\t}"+"\n\n");
			for(int n = 0; n <= N; n++) 
			{
                if(F.get(n).get(0).equals("")) 
				{
                    writer.write("\t\t\t// no aggregate function for grouping variable "+n+"\n");
                    continue;
                }
                writer.write("\t\t\t// aggregate functions for grouping variable "+n+"\n");
                writer.write("\t\t\trs = stat.executeQuery(\"SELECT * FROM sales\");\n");
                writer.write("\t\t\twhile (rs.next()) \n\t\t\t{"+"\n");
                writer.write("\t\t\t\t// get all attributes from a record\n");
                for(i = 0; i < tableStruct.size(); i++) 
				{
                    String attr = tableStruct.get(i);
                    writer.write("\t\t\t\tString "+attr+" = rs.getString(\""+attr+"\");\n");
                }
				writer.write("\n");
                writer.write("\t\t\t\tfor(int i = 0; i < mfStruct.get(\""+V.get(0)+"\").size(); i++) \n\t\t\t\t{"+"\n");
                writer.write("\t\t\t\t\t// Sigma("+n+")"+"\n");
                writer.write("\t\t\t\t\tif("+SIG.get(n)+") \n\t\t\t\t\t{"+"\n");
				for(i = 0; i < F.get(n).size(); i++) 
				{
                    String af = (String)F.get(n).get(i);
                    String[] _af = af.split("_");
                    writer.write("\t\t\t\t\t\t// "+af+"\n");
                    writer.write("\t\t\t\t\t\tString _"+af+" = mfStruct.get(\""+af+"\").get(i)+\"\";"+"\n");
                    writer.write("\t\t\t\t\t\tif(_"+af+".equals(\"null\")) \n\t\t\t\t\t\t{"+"\n");
                    if(_af[0].equals("sum")||_af[0].equals("count")) {
                        writer.write("\t\t\t\t\t\t\t_"+af+" = \"0\";"+"\n");
                    }
                    else {
                        writer.write("\t\t\t\t\t\t\t_"+af+" = "+_af[2]+";"+"\n");
                    }
					writer.write("\t\t\t\t\t\t}\n");
					if(_af[0].equals("sum")) {
                        writer.write("\t\t\t\t\t\tmfStruct.get(\""+af+"\").set(i, ("+table.get(af)+")(Double.parseDouble(_"+af+")+Double.parseDouble("+_af[2]+")));"+"\n");
                    }
                    else if(_af[0].equals("count")) {
                        writer.write("\t\t\t\t\t\tmfStruct.get(\""+af+"\").set(i, ("+table.get(af)+")(Double.parseDouble(_"+af+")+1d));"+"\n");
                    }
                    else if(_af[0].equals("max")) {
                        writer.write("\t\t\t\t\t\tmfStruct.get(\""+af+"\").set(i, ("+table.get(af)+")Math.max(Double.parseDouble(_"+af+"), Double.parseDouble("+_af[2]+")));"+"\n");
                    }
                    else {
                        writer.write("\t\t\t\t\t\tmfStruct.get(\""+af+"\").set(i, ("+table.get(af)+")Math.min(Double.parseDouble(_"+af+"), Double.parseDouble("+_af[2]+")));"+"\n");
                    }
                }
                writer.write("\t\t\t\t\t}"+"\n");
                writer.write("\t\t\t\t}"+"\n");
                writer.write("\t\t\t}"+"\n\n");
				
			}
			
            writer.write("\t\t\t// print output"+"\n");
            writer.write("\t\t\tint rowNum = 0;"+"\n");
            String attrLine = "";
            for(i = 0; i < S.size(); i++) 
			{
                String attr = S.get(i);
                String type = table.get(attr);
                if(type.equals("String")) 
				{
                    attrLine += String.format("%-10s", attr);
                }
                else 
				{
                    attrLine += String.format("%10s", attr);
                }
            }
            writer.write("\t\t\tSystem.out.println(\""+attrLine+"\");"+"\n");
            writer.write("\t\t\tSystem.out.println(\"");
            for(i = 0; i < S.size(); i++) 
			{
                writer.write("==========");
			}
			writer.write("\");\n");
            writer.write("\t\t\tfor(int i = 0; i < mfStruct.get(\""+V.get(0)+"\").size(); i++) \n\t\t\t{"+"\n");
            writer.write("\t\t\t\t// having clause"+"\n");
            writer.write("\t\t\t\tif("+G+") \n\t\t\t\t{"+"\n");
            writer.write("\t\t\t\t\trowNum++;"+"\n");
            for(i = 0; i < S.size(); i++) 
			{
                String attr = S.get(i);
                String[] _attr = attr.split("_");
                String type = table.get(attr);
                writer.write("\t\t\t\t\t// "+attr+"\n");
				 if(_attr[0].equals("avg")&&_attr.length == 3) 
				 {
                    String _sum = "sum_"+_attr[1]+"_"+_attr[2];
                    String _count = "count_"+_attr[1]+"_"+_attr[2];
                    writer.write("\t\t\t\t\tif(((mfStruct.get(\""+_sum+"\").get(i)+\"\").equals(\"null\"))||((mfStruct.get(\""+_count+"\").get(i)+\"\").equals(\"null\"))) \n\t\t\t\t\t{"+"\n");
                    writer.write("\t\t\t\t\t\tSystem.out.print(String.format(\"%10s\", \"null\"));\n");
                    writer.write("\t\t\t\t\t}\n");
                    writer.write("\t\t\t\t\telse \n\t\t\t\t\t{"+"\n");
                    writer.write("\t\t\t\t\t\tSystem.out.print(String.format(\"%10.3f\", Double.parseDouble(("+table.get(_sum)+")mfStruct.get(\""+_sum+"\").get(i)+\"\")/Double.parseDouble(("+table.get(_count)+")mfStruct.get(\""+_count+"\").get(i)+\"\")));\n");
                    writer.write("\t\t\t\t\t}"+"\n");
                }
				else {
                    if(type.equals("Double")) 
					{
                        writer.write("\t\t\t\t\tif((mfStruct.get(\""+attr+"\").get(i)+\"\").equals(\"null\")) \n\t\t\t\t\t{\n");
                        writer.write("\t\t\t\t\t\tSystem.out.print(String.format(\"%10s\", \"null\"));\n");
                        writer.write("\t\t\t\t\t}\n");
                        writer.write("\t\t\t\t\telse \n\t\t\t\t\t{\n");
                        writer.write("\t\t\t\t\t\tSystem.out.print(String.format(\"%10.3f\", ("+type+")mfStruct.get(\""+attr+"\").get(i)));\n");
                        writer.write("\t\t\t\t\t}\n");
                    }
					else if(type.equals("int")) 
					{
                        writer.write("\t\t\t\t\tif((mfStruct.get(\""+attr+"\").get(i)+\"\").equals(\"null\")) \n\t\t\t\t\t{\n");
                        writer.write("\t\t\t\t\t\tSystem.out.print(String.format(\"%10s\", \"null\"));\n");
                        writer.write("\t\t\t\t\t}\n");
                        writer.write("\t\t\t\t\telse \n\t\t\t\t\t{\n");
                        writer.write("\t\t\t\t\t\tSystem.out.print(String.format(\"%10d\", ("+type+")mfStruct.get(\""+attr+"\").get(i)));\n");
                        writer.write("\t\t\t\t\t}\n");
                    }
					else {
                        writer.write("\t\t\t\t\tif((mfStruct.get(\""+attr+"\").get(i)+\"\").equals(\"null\")) \n\t\t\t\t\t{\n");
                        writer.write("\t\t\t\t\t\tSystem.out.print(String.format(\"%-10s\", \"null\"));\n");
                        writer.write("\t\t\t\t\t}\n");
                        writer.write("\t\t\t\t\telse \n\t\t\t\t\t{\n");
                        writer.write("\t\t\t\t\t\tSystem.out.print(String.format(\"%-10s\", ("+type+")mfStruct.get(\""+attr+"\").get(i)));\n");
                        writer.write("\t\t\t\t\t}\n");
                    }
                }
            }
			writer.write("\t\t\t\t\tSystem.out.println();\n");
            writer.write("\t\t\t\t}"+"\n");
            writer.write("\t\t\t}"+"\n\n");
			writer.write("\t\t\tSystem.out.println(\"");
			for(i = 0; i < S.size(); i++) 
			{
                writer.write("==========");
			}
			writer.write("\");\n");
            writer.write("\t\t\tSystem.out.println(\"\\nSuccessfully run. \"+rowNum+\" row(s) affected.\\n\");"+"\n");
            writer.write("\t\t}"+"\n");
            writer.write("\t\tcatch(SQLException e) \n\t\t{"+"\n");
            writer.write("\t\t\tSystem.out.println(\"Database: URL or username or password or table name error!\");"+"\n");
            writer.write("\t\t\te.printStackTrace();"+"\n");
            writer.write("\t\t}"+"\n");
            writer.write("\t}"+"\n");
		    writer.write("}"+"\n");	
			
		}
		catch(Exception e) 
		{
		    e.printStackTrace();
		}
		finally 
		{
		    if(writer != null) 
			{
		        try 
				{
                    writer.close();
                } 
				catch (IOException e) 
				{
                    e.printStackTrace();
                }
		    }
		}
	}
}
			
			
