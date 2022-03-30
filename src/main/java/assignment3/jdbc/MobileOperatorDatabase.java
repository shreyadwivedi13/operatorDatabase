package assignment3.jdbc;

import java.math.BigInteger;
import java.sql.*;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/*
 * problem statement:
 Here are the requirements :-
 We have different mobile operators in India.
 We have to create DB to store Operators details.
 We need to store LINE RANGE data( e.g. we have to define range for each operator e.g. 98720*** - 98729*** belongs to Airtel, 98140*** to 98149*** means Idea etc...)
 Then we have to define Regions for these ranges too i.e. this RANGE belongs to Punjab airtel and this range belongs to Haryana Airtel.
 As of now , we can assume there is no concept of MNP i.e. Mobile number Portability.
 We have to store all SMSs those sent from any Indian mobile number to any Indian mobile number.
 Other message details to be store :From,To,From Operator,To Operator,sent time, received time,deliveryStatus.
 Queries :-
 We have to print all messages sent from * number To any number.
 We have to print all messages received by * from any number.
 We have to print all messages sent from XXXX to YYYY.
 We have to print all messages received by * from Punjab number.
 We have to print all messages received by * from Airtel Punjab number.
 We have to print all messages received from 98786912** ( here * mean it can by any two digit, i.e. messages from 9878691291,92,93,94,95 etc..) by **.
 We have to print all messages those were sent from Punjab number but FAILED.
 You have to design database and then write Java code to fullfil all the requirements.
 
 @author shreya.dwivedi
 */

public class MobileOperatorDatabase {

	public static final Logger log = LogManager.getLogger(MobileOperatorDatabase.class.getName());

	/*
	 * @param - connection
	 * 
	 * @throws Exception checking if in the database the tables already exists or
	 * not, if not then creating a tables with required columnName and then calling
	 * their table creation and insertion function otherwise directly the query
	 * function will be called from main.
	 * 
	 */
	public static void checkTables(Connection con) throws Exception {

		ResultSet rs = null;
		try {
			DatabaseMetaData databaseMetadata = con.getMetaData();
			log.info("Checking if a operator range table already exists");
			rs = databaseMetadata.getTables(null, null, "operator_range", null);
			if (rs.next()) {
				// System.out.println(rs.next());
				log.info("operator_range exists");
			} else {
				log.info("operator_range doesn't exists");
				createAndPolulateOperatorRange(con);
			}
			log.info("Checking if a operator region table already exists");
			rs = databaseMetadata.getTables(null, null, "operator_region", null);
			if (rs.next()) {
				// System.out.println(rs.next());
				log.info("operator_region exists");
			} else {
				log.info("operator_region doesn't exists");
				createAndPolulateOperatorRegion(con);
			}
			log.info("Checking if a msg_info table already exists");
			rs = databaseMetadata.getTables(null, null, "msg_info", null);
			if (rs.next()) {
				// System.out.println(rs.next());
				log.info("msg_info exists");
			} else {
				log.info("msg_info doesn't exists");
				createAndPopulateMSGTable(con);
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new Exception("Couldn't check the tables, some error occured");

		}
	}

	/*
	 * @param ->connection
	 * 
	 * @throws Exception
	 * 
	 * this function will create operator_range table which will have specified
	 * ranges divided in operators if it runs properly otherwise will throw and
	 * error.
	 * 
	 */
	public static void createAndPolulateOperatorRange(Connection con) throws Exception {

		try {
			Statement statement = con.createStatement();
			// creating table1 ie operator_range which will have the ranges of the
			// operators
			String sql = "CREATE TABLE operator_range " + "(ranges INTEGER , " + " operator VARCHAR(255), "
					+ " PRIMARY KEY ( ranges )) ";

			statement.executeUpdate(sql);
			log.info(" operator_range table created successfully!");
			PreparedStatement ps = con.prepareStatement("insert into operator_range values(?,?)");
			// creating table to simplify the ranges of phone numbers and their operators.
			// specifying range for Airtel operator
			int airtelRange = 9872;
			ps.setInt(1, airtelRange);
			ps.setString(2, "Airtel");
			ps.addBatch();

			// specifying range for Idea operator
			int ideaRange = 9814;

			ps.setInt(1, ideaRange);
			ps.setString(2, "Idea");
			ps.addBatch();

			// specifying range for Vodafone operator
			int vodaRange = 9867;

			ps.setInt(1, vodaRange);
			ps.setString(2, "Vodafone");
			ps.addBatch();

			// specifying range for Jio operator
			int jioRange = 98320;

			ps.setInt(1, jioRange);
			ps.setString(2, "Jio");
			ps.addBatch();
			ps.executeBatch();
			log.info("values in operator_range table have been inserted successfully!");

		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new Exception("some error occured with operator range table");
		}
	}

	/*
	 * @param ->connection
	 * 
	 * @throws Exception second table function create operator_region table which
	 * will have info about operator region and region id.
	 */
	public static void createAndPolulateOperatorRegion(Connection con) throws Exception {
		{
			try (Statement stmt = con.createStatement();) {
				String sql = "CREATE TABLE operator_region" + "(region_id INT not NULL, " + " region VARCHAR(255), "
						+ " PRIMARY KEY (region_id ))";

				stmt.executeUpdate(sql);
				log.info("Created operator region table successfully! ");
				String[] regions = new String[] { "UP", "HP", "MP", "Kerala", "Goa", "Assam", "Punjab", "Haryana",
						"Bihar", "Karnataka" };// storing all the operator regions in a string array.

				PreparedStatement prepapredStatement = con
						.prepareStatement("INSERT INTO operator_region" + " VALUES (?, ?)");
				// iterating over the string array to assign them with their region ids.
				for (int i = 0; i < regions.length; i++) {
					prepapredStatement.setInt(1, i);
					prepapredStatement.setString(2, regions[i]);
					prepapredStatement.addBatch();
				}

				prepapredStatement.executeBatch();

				log.info("values in operator_region table have been inserted successfully!");
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				throw new Exception("some error occured with operator_region table");

			}
		}
	}

	/*
	 * @param ->connection
	 * 
	 * @throws Exception second table function create msg_info table which will have
	 * info about msg sent and received.
	 */
	public static void createAndPopulateMSGTable(Connection con) throws Exception {

		try {
			Statement statement = con.createStatement();
			// query to create table.
			String sql1 = "CREATE TABLE msg_info " + "(sentFrom BIGINT not NULL, " + " sentTo BIGINT not NULL, "
					+ " message VARCHAR(255) not NULL," + " sentTime TIMESTAMP, " + " recievedTime TIMESTAMP  ,"
					+ " deliveryStatus VARCHAR(255))";

			statement.execute(sql1);
			log.info("value inmsg_info table created successfully!");
			// inserting values in the second table.

			statement.addBatch("INSERT into msg_info values(9872900001,9814900000,'Hey',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9814000000,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9867900301,9814500000,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9832777777,9814000000,'??',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9814200000,9832777777,'HBD!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9832777777,'HNY!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9814078900,9814000000,'HI!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9814078900,9814000000,'HBD!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9814000000,'HNY!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9814200000,'BYE!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9814000000,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9867900301,9814500000,'BYE!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9832777777,9814000000,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9814200000,9832777777,'NO!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9832777777,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9814078900,9814000000,'YES!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9814078900,9814000000,'OKAY!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9814000000,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872600301,9814200000,'Hey!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9814678900,9814000000,'OKAY!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9872600301,9814900000,'Hey!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9872600301,9814200000,'Hey!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9814660000,9872900301,'NO!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872660302,9872900301,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9814678900,9872900301,'YES!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9814078900,9872900301,'OKAY!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872600351,9872900301,'TTYL!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872800301,9872691266,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872600301,9872691290,'Hii!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9872600301,9878691289,'Hii!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9872600361,9872900301,'Hail!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872600301,9872900301,'OKAY!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9872691266,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9878691290,'Hii!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9872900301,9878691289,'Hii!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9872900361,9878691301,'Hail!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872900301,9878691201,'OKAY!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872600303,9872600301,'OKAY!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872600361,9872600301,'Hey!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872660301,9872600301,'Hii!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9872670301,9872600301,'Hii!',NOW(),NOW(),'Failed')");
			statement.addBatch("INSERT into msg_info values(9872640361,9872600301,'Hail!',NOW(),NOW(),'Received')");
			statement.addBatch("INSERT into msg_info values(9872678301,9872600301,'OKAY!',NOW(),NOW(),'Received')");
			statement.executeBatch();
			log.info("value in msg_info table inserted successfully!");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new Exception("some error occured with msg_info table.");
		}
	}

	/*
	 * @param -> connection
	 * 
	 * @throws Exception
	 * 
	 * this function will execute all queries.
	 */
	public static void finalQueryOutputs(Connection con) throws Exception {
		Statement statement = con.createStatement();
		try (Scanner ob = new Scanner(System.in);) {
			int choice;
			boolean countinuationChoice = true;
			do {

				System.out.println("Enter the specified number to execute following queries and 0 to exit : ");
				System.out.println("1. Enter 1 to Print all messages sent from a given number. ");
				System.out.println("2. Enter 2 to Print all messages to a given number. ");
				System.out.println("3. Enter 3 to Print all messages sent between two years. ");
				System.out.println("4. Enter 4 to Print all messages receieved by given number from punjab number. ");
				System.out.println(
						"5. Enter 5 to Print all messages receieved by given number from airtel punjab number. ");
				System.out.println(
						"6. Enter 6 to Print all messages sent by 98786912**, (Where ** could be any two digits). ");
				System.out.println("7. Enter 7 to Print all messages sent from punjab but failed . ");
				System.out.println("8. Enter 8 to exit. ");

				choice = ob.nextInt();
				switch (choice) {
				case 1: {
					System.out.println("enter sender's number : ");
					long sender = ob.nextLong();
//					sample input->9872600301
					String query1 = "SELECT message from msg_info where sentFrom = " + sender;
					ResultSet rs = statement.executeQuery(query1);
					int msg_count = statement.getUpdateCount();
					while (rs.next()) {
						log.info("Message : " + rs.getString("message"));
						if (msg_count == -1) {
							log.info("No messages found.");
						}
					}
					break;
				}
				case 2: {
					System.out.println("enter recipient's number : ");
					long recipient = ob.nextLong();
//					sample input->9872600301
					String query2 = "SELECT message from msg_info where sentFrom = " + recipient;
					ResultSet rs = statement.executeQuery(query2);
					int msg_count = statement.getUpdateCount();
					while (rs.next()) {
						log.info("Message : " + rs.getString("message"));
						if (msg_count == -1) {
							log.info("No messages found.");
						}
					}
					break;
				}
				case 3: {
					System.out.println("enter first year : ");
					long year1 = ob.nextInt();
//					sample input ->2021
					System.out.println("enter second year : ");
					long year2 = ob.nextInt();
//					sample input -> 2022
					String query3 = " SELECT message from msg_info where YEAR(sentTime) between " + year1 + " and "
							+ year2;
					ResultSet rs = statement.executeQuery(query3);
					int msg_count = statement.getUpdateCount();
					while (rs.next()) {
						log.info("Message : " + rs.getString("message"));
						if (msg_count == -1) {
							log.info("No messages found.");
						}
					}
					break;
				}
				case 4: {
					System.out.println("enter recipient's number : ");
					long recipient = ob.nextLong();
//					sample input->9872600301
					String query4 = "Select a.message FROM msg_info A inner join operator_region b on FLOOR(((a.sentFrom/100000) %10)) = b.region_id "
							+ "WHERE a.sentTo = " + recipient + " and b.region = 'Punjab'";
					ResultSet rs = statement.executeQuery(query4);
					int msg_count = statement.getUpdateCount();
					while (rs.next()) {
						log.info("Message : " + rs.getString("message"));
						if (msg_count == -1) {
							log.info("No messages found.");
						}
					}
					break;
				}
				case 5: {
					System.out.println("enter recipient's number : ");
					long recipient = ob.nextLong();
//					sample input->9872600301
					String query5 = "Select A.message FROM msg_info A inner join operator_region b on FLOOR(((A.sentFrom/100000) %10)) = b.region_id inner join operator_range c on FLOOR(a.sentFrom/1000000) = c.ranges where  b.region = 'Punjab'and c.operator = 'Airtel' and a.sentTo = "
							+ recipient;

					ResultSet rs = statement.executeQuery(query5);
					int msg_count = statement.getUpdateCount();
					while (rs.next()) {
						log.info("Message : " + rs.getString("message"));
						if (msg_count == -1) {
							log.info("No messages found.");
						}
					}
					break;
				}
				case 6: {
					System.out.println("Enter the sender's number : ");
					long sender = ob.nextLong();
					// sample input->9872900301
					String query6 = "Select MESSAGE FROM msg_info WHERE sentTo>9878691199 and sentTo<9878691300 and sentFrom = "
							+ sender;
					// String query6 = "SELECT message from msg_info where sentTo>9878691199 and
					// sentTo<9878691300 and sentFrom = 9872900301";
					ResultSet rs = statement.executeQuery(query6);
					int msg_count = statement.getUpdateCount();
					while (rs.next()) {
						log.info("Message : " + rs.getString("message"));
						if (msg_count == -1) {
							log.info("No messages found.");
						}
					}
					break;
				}
				case 7: {
					String query7 = "Select A.MESSAGE FROM msg_info A INNER JOIN operator_region b on FLOOR(((a.sentFrom/100000) %10)) = b.region_id "
							+ "WHERE a.deliverystatus = 'failed' and b.region = 'Punjab'";

					ResultSet rs = statement.executeQuery(query7);
					int msg_count = statement.getUpdateCount();
					while (rs.next()) {
						log.info("Message : " + rs.getString("message"));
						if (msg_count == -1) {
							log.info("No messages found.");
						}
					}
					break;
				}
				case 8: {
					System.out.println("Exiting query loop.");
					countinuationChoice = false;
					break;
				}
				default: {
					System.out.println("Wrong input.");
				}

				}

			} while (countinuationChoice == true);

		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new Exception("some error occured while executing the queries.");
		}
	}

	/*
	 * fetches getFile function from readConfig file class
	 */
	public static void resourceIntializer() throws Exception {
		try {
			ReadConfigFile.getFile();
		} catch (Exception e) {
			throw new Exception("couldn't load the configFile.");
		}
	}

	public static void main(String[] args) throws Exception {
		// Class.forName("com.mysql.jdbc.Driver");
		try {
			resourceIntializer();

			String databaseURL = ReadConfigFile.getResources("databaseURL");
			log.info("databaseURL" + databaseURL);
			String user = ReadConfigFile.getResources("user");
			log.info("username:" + user);
			String password = ReadConfigFile.getResources("password");
			log.info("password:" + password);
//			String dbName = "network_operator";
			Connection con = DriverManager.getConnection(databaseURL, user, password);

			checkTables(con);
			finalQueryOutputs(con);

		} catch (Exception e) {
			log.fatal("some error occured." + e);
		}

	}
}
