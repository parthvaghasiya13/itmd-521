import java.sql.*;

public class javaCode
		{
			public static void main(String args[])
				{
					int maximumTemperature = 0;
					try
						{
							Class.forName("com.mysql.jdbc.Driver");
							Connection connnection1=DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/itmd521","root","itmd521");
							Statement statement1=con.createStatement();
							ResultSet resultset1=statement1.executeQuery("select temp from records where temp=(select max(temp)from records where temp>"+99+" and temp!="+9999+");");
							while(resultset1.next()){
							maximumTemperature = Integer.parseInt(rs.getString(1));
							System.out.println("Maximum temperature of 1985 dataset is:"+maximumTemperature);
						}

					connection1.close();
						}
				
					catch(Exception e)
					
					{
						System.out.println(e);
						}
				}
}

