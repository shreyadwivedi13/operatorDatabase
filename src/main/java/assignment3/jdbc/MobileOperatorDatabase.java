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
	public final static String DEFAULT_NAME_OPERATOR_RANGE = "operator_range";
	// default name
	public final static String DEFAULT_NAME_OPERATOR_REGION = "operator_region";
	// default name
	public final static String DEFAULT_NAME_MSG_INFO = "msg_info";
	// default name
	/*
	 * @param - connection
	 * 
	 * @throws Exception checking if in the database the tables already exists or
	 * not, if not then creating a tables with required columnName and then calling
	 * their table creation and insertion function otherwise directly the query
	 * function will be called from main.
	 * 
	 */
	public static void checkTables(Connection con, String[] table_name) throws Exception {
		ResultSet rs = null;
		try {
			DatabaseMetaData databaseMetadata = con.getMetaData();
			for (int i = 0; i < table_name.length; i++) {
				log.info("Checking if " + table_name[i] + " already exists.");
				rs = databaseMetadata.getTables(null, null, table_name[i], null);
				if (rs.next()) {
					// System.out.println(rs.next());
					log.info(table_name[i] + " exists");
				} else {
					log.info(table_name[i] + " doesn't exists");
					if (i == 0) {
						createAndPolulateOperatorRange(con);
					} else if (i == 1) {
						createAndPolulateOperatorRegion(con);
					} else {
						createAndPopulateMSGTable(con);
						;
					}
				}
			}
		} catch (Exception e) {
			log.info(e.getMessage());
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
			// creating table to simplify the ranges of phone numbers and their operators
			int[] rangeArray = new int[] { 9872, 9814, 9867, 9832 }; // array of operator ranges
			String[] operatorNames = new String[] { "Airtel", "Idea", "Vodafone", "Jio" };// array of operator names
			for (int i = 0; i < rangeArray.length; i++) {
				ps.setInt(1, rangeArray[i]);
				ps.setString(1, operatorNames[i]);
				ps.addBatch();
			}
			ps.executeBatch();
			log.info("values in operator_range table have been inserted successfully!");

		} catch (Exception e) {
			log.info(e.getMessage());
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
				log.info(e.getMessage());
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
			long[] senderPh = new long[] { 9872900001L, 9872900301L, 9862900301L, 9832777777L, 9814200000L, 9872900301L,
					9814078900L, 9814078900L, 9872900301L, 9872900301L, 9872900301L, 9872900301L, 9832777777L,
					9814200000L, 9872900301L, 9814078900L, 9814078900L, 9872900301L, 9872900301L, 9814678900L,
					9872900301L, 9872900301L, 9814660000L, 9872900302L, 981468900L, 9814678900L, 9872600351L,
					9872800301L, 9872600301L, 9872600301L, 9872600361L, 9872600301L, 9872900301L, 9872900301L,
					9872900301L, 9872900301L, 9872900361L, 9872900301L, 9872600361L, 9872660301L, 9872670301L,
					9872640361L };
			long[] recipientPh = new long[] { 9814900000L, 9814000000L, 9814500000L, 9814900000L, 9832777777L,
					9832777777L, 9814000000L, 9814000000L, 9814000000L, 9814200000L, 9814000000L, 9814500000L,
					9814000000L, 9832777777L, 9832777777L, 9814000000L, 9814000000L, 9814000000L, 9814200000L,
					9814000000L, 9814900000L, 9814200000L, 9872900301L, 9872900301L, 9872900301L, 9872900301L,
					9872900301L, 9872691266L, 9872691290L, 9878691289L, 9872900301L, 9872900301L, 9872691266L,
					9878691290L, 9878691289L, 9878691301L, 9878691201L, 9872600301L, 9872600301L, 9872600301L,
					9872600301L, 9872600301L };
			String[] msges = new String[] { "hey", "hi", "no", "stop", "ok", "what?", "!!", "bye", "okay", "how", "hey",
					"hi", "no", "stop", "ok", "what?", "!!", "bye", "okay", "how", "hey", "hi", "no", "stop", "ok",
					"what?", "!!", "bye", "okay", "how", "hey", "hi", "no", "stop", "ok", "what?", "!!", "bye", "okay",
					"how", "hbd", "hbd" };
			String[] status = new String[] { "Received", "Received", "Received", "Received", "Received", "Received",
					"Received", "Received", "Received", "Received", "Received", "Received", "Received", "Received",
					"Received", "Failed", "Received", "Received", "Received", "Received", "Failed", "Received",
					"Received", "Received", "Received", "Failed", "Failed", "Failed", "Failed", "Failed", "Failed",
					"Received", "Received", "Received", "Received", "Failed", "Failed", "Failed", "Failed", "Failed",
					"Failed", "Failed" };
			PreparedStatement prepapredStatement = con
					.prepareStatement("INSERT INTO msg_info" + " VALUES (?, ?, ?,NOW(),NOW(), ?");
			for (int i = 0; i < senderPh.length; i++) {
				prepapredStatement.setLong(1, senderPh[i]);
				prepapredStatement.setLong(2, recipientPh[i]);
				prepapredStatement.setString(3, msges[i]);
				prepapredStatement.setString(6, status[i]);
				prepapredStatement.addBatch();
			}

			prepapredStatement.executeBatch();

			log.info("value in msg_info table inserted successfully!");
		} catch (Exception e) {
			log.info(e.getMessage());
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
		try (Scanner scanner = new Scanner(System.in);) {
			int choice;
			boolean countinuationChoice = true;
			do {

				log.info("Enter the specified number to execute following queries and 0 to exit : ");
				log.info("1. Enter 1 to Print all messages sent from a given number. ");
				log.info("2. Enter 2 to Print all messages to a given number. ");
				log.info("3. Enter 3 to Print all messages sent between two years. ");
				log.info("4. Enter 4 to Print all messages receieved by given number from punjab number. ");
				log.info("5. Enter 5 to Print all messages receieved by given number from airtel punjab number. ");
				log.info("6. Enter 6 to Print all messages sent by 98786912**, (Where ** could be any two digits). ");
				log.info("7. Enter 7 to Print all messages sent from punjab but failed . ");
				log.info("8. Enter 8 to exit. ");

				choice = scanner.nextInt();
				switch (choice) {
				case 1: {
					query1(scanner, con, statement);
					break;
				}
				case 2: {
					
					query2(scanner, con, statement);
					break;
				}
				case 3: {
					
					
					query3(scanner, con, statement);
					break;
				}
				case 4: {
					
					query4(scanner, con, statement);
					break;
				}
				case 5:{
					query5(scanner, con, statement);
					break;
				}
				case 6: {
					
					query6(scanner, con, statement);
					break;
				}
				case 7: {
					
					query7(scanner, con, statement);
					break;
				}
				case 8: {
					log.info("Exiting query loop.");
					countinuationChoice = false;
					break;
				}
				default: {
					log.info("Wrong input.");
				}

				}

			} while (countinuationChoice == true);

		}catch(

	Exception e)
	{
		log.info(e.getMessage());
		throw new Exception("some error occured while executing the queries.");
	}
	}

	public static void query1(Scanner scanner, Connection con, Statement statement) throws Exception {
		log.info("enter sender's number : ");
		long sender = scanner.nextLong();
//		sample input->9872600301
		String query1 = "SELECT message from msg_info where sentFrom = " + sender;
		ResultSet rs = statement.executeQuery(query1);
		displayMessages(rs);
	}

	public static void query2(Scanner scanner, Connection con, Statement statement) throws Exception {
		log.info("enter recipient's number : ");
		long recipient = scanner.nextLong();
//		sample input->9872600301
		String query2 = "SELECT message from msg_info where sentFrom = " + recipient;
		ResultSet rs = statement.executeQuery(query2);
		displayMessages(rs);

	}

	public static void query3(Scanner scanner, Connection con, Statement statement) throws Exception {
		log.info("enter first year : ");
		long year1 = scanner.nextInt();
//		sample input ->2021
		log.info("enter second year : ");
		long year2 = scanner.nextInt();
//		sample input -> 2022
		String query3 = " SELECT message from msg_info where YEAR(sentTime) between " + year1 + " and " + year2;
		ResultSet rs = statement.executeQuery(query3);
		displayMessages(rs);
	}

	public static void query4(Scanner scanner, Connection con, Statement statement) throws Exception {
		log.info("enter recipient's number : ");
		long recipient = scanner.nextLong();
//		sample input->9872600301
		String query4 = "Select msg.message FROM msg_info msg inner join operator_region op on FLOOR(((msg.sentFrom/100000) %10)) = op.region_id "
				+ "WHERE msg.sentTo = " + recipient + " and op.region = 'Punjab'";
		ResultSet rs = statement.executeQuery(query4);
		displayMessages(rs);
	}

	public static void query5(Scanner scanner, Connection con, Statement statement) throws Exception {
		log.info("enter recipient's number : ");
		long recipient = scanner.nextLong();
//		sample input->9872600301
		String query5 = "Select msg.message FROM msg_info msg inner join operator_region op on FLOOR(((msg.sentFrom/100000) %10)) = op.region_id inner join operator_range opr on FLOOR(a.sentFrom/1000000) = opr.ranges where  op.region = 'Punjab'and opr.operator = 'Airtel' and msg.sentTo = "
				+ recipient;

		ResultSet rs = statement.executeQuery(query5);
		displayMessages(rs);
	}

	public static void query6(Scanner scanner, Connection con, Statement statement) throws Exception {
		log.info("Enter the sender's number : ");
		long sender = scanner.nextLong();
		// sample input->9872900301
		String query6 = "Select MESSAGE FROM msg_info WHERE sentTo>9878691199 and sentTo<9878691300 and sentFrom = "
				+ sender;
		// String query6 = "SELECT message from msg_info where sentTo>9878691199 and
		// sentTo<9878691300 and sentFrom = 9872900301";
		ResultSet rs = statement.executeQuery(query6);
		displayMessages(rs);
	}

	public static void query7(Scanner scanner, Connection con, Statement statement) throws Exception {
		String query7 = "Select msg.MESSAGE FROM msg_info msg INNER JOIN operator_region op on FLOOR(((msg.sentFrom/100000) %10)) = op.region_id "
				+ "WHERE msg.deliverystatus = 'failed' and op.region = 'Punjab'";

		ResultSet rs = statement.executeQuery(query7);
		displayMessages(rs);
	}

	public static void displayMessages(ResultSet rs) throws Exception {
		while (rs.next()) {
			log.info("Message : " + rs.getString("message"));
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

			String databaseURL = ReadConfigFile.getResourceValues("databaseURL");
			log.info("databaseURL" + databaseURL);
			String user = ReadConfigFile.getResourceValues("user");
			log.info("username:" + user);
			String password = ReadConfigFile.getResourceValues("password");
			log.info("password:" + password);
//			String dbName = "network_operator";
			Connection con = DriverManager.getConnection(databaseURL, user, password);
			String table_name[] = new String[] { "operator_range", "operator_region", "msg_info" };
			checkTables(con, table_name);
			finalQueryOutputs(con);

		} catch (Exception e) {
			log.fatal("some error occured." + e);
		}

	}
}
