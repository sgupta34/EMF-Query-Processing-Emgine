import java.sql.*;
import java.util.*;

public class GeneratedQuery 
{
	public static Map<String, List> mfStruct = new HashMap<>();
	public static ResultSet rs = null;
	public static void main(String[] args) 
	{
		String usr ="postgres";
		String pass ="qwerty";
		String url ="jdbc:postgresql://localhost:5432/CS562";
		// define MF-Struct
		mfStruct.put("prod", new ArrayList<String>());
		mfStruct.put("quant", new ArrayList<Integer>());
		mfStruct.put("count_1_prod", new ArrayList<Integer>());
		mfStruct.put("count_2_prod", new ArrayList<Integer>());
		//DATABASE Connection Set up
		try 
		{
			Class.forName("org.postgresql.Driver");
			System.out.println("Database Successfully Loaded.");
		}
		catch(Exception e) 
		{
			System.out.println("Database Loading FAILED.");
			e.printStackTrace();
		}

		try 
		{
			Connection conn = DriverManager.getConnection(url, usr, pass);
			System.out.println("Database Connection SUCCESSFUL.\n");

			Statement stat = conn.createStatement();

			rs = stat.executeQuery("SELECT * FROM sales");
			while (rs.next()) 
			{

				String prod = rs.getString("prod");
				String quant = rs.getString("quant");
				boolean flag = true;
				for(int i = 0; i < mfStruct.get("prod").size(); i++)
				{
					if(prod.equals((String)mfStruct.get("prod").get(i)+"")&&quant.equals((int)mfStruct.get("quant").get(i)+"")) 
					{
						flag = false;
						break;
					}
				}
				// add a new combination of grouping attributes
				if(flag) 
				{
					mfStruct.get("prod").add(prod+"");
					mfStruct.get("quant").add(Integer.parseInt(quant+""));
					mfStruct.get("count_1_prod").add(null);
					mfStruct.get("count_2_prod").add(null);
				}
			}

			// no aggregate function for grouping variable 0
			// aggregate functions for grouping variable 1
			rs = stat.executeQuery("SELECT * FROM sales");
			while (rs.next()) 
			{
				// get all attributes from a record
				String cust = rs.getString("cust");
				String prod = rs.getString("prod");
				String day = rs.getString("day");
				String month = rs.getString("month");
				String year = rs.getString("year");
				String state = rs.getString("state");
				String quant = rs.getString("quant");

				for(int i = 0; i < mfStruct.get("prod").size(); i++) 
				{
					// Sigma(1)
					if((!(mfStruct.get("prod").get(i)+"").equals("null"))&&(prod.equals((String)mfStruct.get("prod").get(i)+""))) 
					{
						// count_1_prod
						String _count_1_prod = mfStruct.get("count_1_prod").get(i)+"";
						if(_count_1_prod.equals("null")) 
						{
							_count_1_prod = "0";
						}
						mfStruct.get("count_1_prod").set(i, (int)(Double.parseDouble(_count_1_prod)+1d));
					}
				}
			}

			// aggregate functions for grouping variable 2
			rs = stat.executeQuery("SELECT * FROM sales");
			while (rs.next()) 
			{
				// get all attributes from a record
				String cust = rs.getString("cust");
				String prod = rs.getString("prod");
				String day = rs.getString("day");
				String month = rs.getString("month");
				String year = rs.getString("year");
				String state = rs.getString("state");
				String quant = rs.getString("quant");

				for(int i = 0; i < mfStruct.get("prod").size(); i++) 
				{
					// Sigma(2)
					if((!(mfStruct.get("prod").get(i)+"").equals("null"))&&(!(mfStruct.get("quant").get(i)+"").equals("null"))&&(prod.equals((String)mfStruct.get("prod").get(i)+""))&&(Double.parseDouble(quant)<Double.parseDouble((int)mfStruct.get("quant").get(i)+""))) 
					{
						// count_2_prod
						String _count_2_prod = mfStruct.get("count_2_prod").get(i)+"";
						if(_count_2_prod.equals("null")) 
						{
							_count_2_prod = "0";
						}
						mfStruct.get("count_2_prod").set(i, (int)(Double.parseDouble(_count_2_prod)+1d));
					}
				}
			}

			// print output
			int rowNum = 0;
			System.out.println("prod                       quant");
			System.out.println("================================");
			for(int i = 0; i < mfStruct.get("prod").size(); i++) 
			{
				// having clause
				if((!(mfStruct.get("count_2_prod").get(i)+"").equals("null"))&&(!(mfStruct.get("count_1_prod").get(i)+"").equals("null"))&&(((int)mfStruct.get("count_2_prod").get(i)+"").equals((int)mfStruct.get("count_1_prod").get(i)/2+""))) 
				{
					rowNum++;
					// prod
					if((mfStruct.get("prod").get(i)+"").equals("null")) 
					{
						System.out.print(String.format("%-16s", "null"));
					}
					else 
					{
						System.out.print(String.format("%-16s", (String)mfStruct.get("prod").get(i)));
					}
					// quant
					if((mfStruct.get("quant").get(i)+"").equals("null")) 
					{
						System.out.print(String.format("%16s", "null"));
					}
					else 
					{
						System.out.print(String.format("%16d", (int)mfStruct.get("quant").get(i)));
					}
					System.out.println();
				}
			}

			System.out.println("\nSuccessfully run. "+rowNum+" row(s) affected.\n");
		}
		catch(SQLException e) 
		{
			System.out.println("Database: URL or username or password or table name error!");
			e.printStackTrace();
		}
	}
}
