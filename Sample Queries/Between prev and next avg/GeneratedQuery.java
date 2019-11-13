import java.sql.*;
import java.util.*;

@SuppressWarnings("unchecked")
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
		mfStruct.put("month", new ArrayList<Integer>());
		mfStruct.put("sum_1_quant", new ArrayList<Integer>());
		mfStruct.put("count_1_quant", new ArrayList<Integer>());
		mfStruct.put("sum_2_quant", new ArrayList<Integer>());
		mfStruct.put("count_2_quant", new ArrayList<Integer>());
		mfStruct.put("count_3_quant", new ArrayList<Integer>());
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
				String month = rs.getString("month");
				boolean flag = true;
				for(int i = 0; i < mfStruct.get("prod").size(); i++)
				{
					if(prod.equals((String)mfStruct.get("prod").get(i)+"")&&month.equals((int)mfStruct.get("month").get(i)+"")) 
					{
						flag = false;
						break;
					}
				}
				// add a new combination of grouping attributes
				if(flag) 
				{
					mfStruct.get("prod").add(prod+"");
					mfStruct.get("month").add(Integer.parseInt(month+""));
					mfStruct.get("sum_1_quant").add(null);
					mfStruct.get("count_1_quant").add(null);
					mfStruct.get("sum_2_quant").add(null);
					mfStruct.get("count_2_quant").add(null);
					mfStruct.get("count_3_quant").add(null);
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
					if((!(mfStruct.get("prod").get(i)+"").equals("null"))&&(!(mfStruct.get("month").get(i)+"").equals("null"))&&(prod.equals((String)mfStruct.get("prod").get(i)+""))&&(month.equals((int)mfStruct.get("month").get(i)-1+""))) 
					{
						// sum_1_quant
						String _sum_1_quant = mfStruct.get("sum_1_quant").get(i)+"";
						if(_sum_1_quant.equals("null")) 
						{
							_sum_1_quant = "0";
						}
						mfStruct.get("sum_1_quant").set(i, (int)(Double.parseDouble(_sum_1_quant)+Double.parseDouble(quant)));
						// count_1_quant
						String _count_1_quant = mfStruct.get("count_1_quant").get(i)+"";
						if(_count_1_quant.equals("null")) 
						{
							_count_1_quant = "0";
						}
						mfStruct.get("count_1_quant").set(i, (int)(Double.parseDouble(_count_1_quant)+1d));
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
					if((!(mfStruct.get("prod").get(i)+"").equals("null"))&&(!(mfStruct.get("month").get(i)+"").equals("null"))&&(prod.equals((String)mfStruct.get("prod").get(i)+""))&&(month.equals((int)mfStruct.get("month").get(i)+1+""))) 
					{
						// sum_2_quant
						String _sum_2_quant = mfStruct.get("sum_2_quant").get(i)+"";
						if(_sum_2_quant.equals("null")) 
						{
							_sum_2_quant = "0";
						}
						mfStruct.get("sum_2_quant").set(i, (int)(Double.parseDouble(_sum_2_quant)+Double.parseDouble(quant)));
						// count_2_quant
						String _count_2_quant = mfStruct.get("count_2_quant").get(i)+"";
						if(_count_2_quant.equals("null")) 
						{
							_count_2_quant = "0";
						}
						mfStruct.get("count_2_quant").set(i, (int)(Double.parseDouble(_count_2_quant)+1d));
					}
				}
			}

			// aggregate functions for grouping variable 3
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
					// Sigma(3)
					if((!(mfStruct.get("prod").get(i)+"").equals("null"))&&(!(mfStruct.get("month").get(i)+"").equals("null"))&&(!(mfStruct.get("sum_1_quant").get(i)+"").equals("null"))&&(!(mfStruct.get("count_1_quant").get(i)+"").equals("null"))&&(!(mfStruct.get("sum_2_quant").get(i)+"").equals("null"))&&(!(mfStruct.get("count_2_quant").get(i)+"").equals("null"))&&(prod.equals((String)mfStruct.get("prod").get(i)+""))&&(month.equals((int)mfStruct.get("month").get(i)+""))&&(Double.parseDouble(quant)>Double.parseDouble((Double.parseDouble((int)mfStruct.get("sum_1_quant").get(i)+"")/Double.parseDouble((int)mfStruct.get("count_1_quant").get(i)+""))+""))&&(Double.parseDouble(quant)<Double.parseDouble((Double.parseDouble((int)mfStruct.get("sum_2_quant").get(i)+"")/Double.parseDouble((int)mfStruct.get("count_2_quant").get(i)+""))+""))) 
					{
						// count_3_quant
						String _count_3_quant = mfStruct.get("count_3_quant").get(i)+"";
						if(_count_3_quant.equals("null")) 
						{
							_count_3_quant = "0";
						}
						mfStruct.get("count_3_quant").set(i, (int)(Double.parseDouble(_count_3_quant)+1d));
					}
				}
			}

			// print output
			int rowNum = 0;
			System.out.println("prod                       month   count_3_quant");
			System.out.println("================================================");
			for(int i = 0; i < mfStruct.get("prod").size(); i++) 
			{
				// having clause
				if(true) 
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
					// month
					if((mfStruct.get("month").get(i)+"").equals("null")) 
					{
						System.out.print(String.format("%16s", "null"));
					}
					else 
					{
						System.out.print(String.format("%16d", (int)mfStruct.get("month").get(i)));
					}
					// count_3_quant
					if((mfStruct.get("count_3_quant").get(i)+"").equals("null")) 
					{
						System.out.print(String.format("%16s", "null"));
					}
					else 
					{
						System.out.print(String.format("%16d", (int)mfStruct.get("count_3_quant").get(i)));
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
