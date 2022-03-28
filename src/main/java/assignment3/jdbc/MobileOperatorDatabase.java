package assignment3.jdbc;

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


	public static void checkDatabase(String databaseURL, String dbName, String user, String password) throws Exception {
		/*
		 * checking if the database already exists or not, if not then creating a
		 * database with required dbName.
		 */
		ResultSet rs = null;

		try (Connection con = DriverManager.getConnection(databaseURL, user, password);) {

			if (con != null) {

				log.info("Checking if a database by this name already exists");
				rs = con.getMetaData().getCatalogs();
				boolean exists = false;
				while (rs.next()) {
					String catalogs = rs.getString(1);

					if (dbName.equals(catalogs)) {
						// System.out.println("the database " + dbName + " exists");
						exists = true;
						break;
					} else {
						exists = false;
						// System.out.println("?");
					}
				}
				log.info("db exists: " + exists);

				if (exists == false) {
					log.info("no database found with this name so creating new database");
					Statement statement = con.createStatement();
					String sql = "CREATE DATABASE " + dbName;
					statement.executeUpdate(sql);
					CreateAndPopulateOperatorTable(databaseURL, dbName, user, password);
					log.info("Database with name " + dbName + " created.");

				}
			}
		} catch (Exception e) {
			throw new Exception("couldn't create the database, ERROR occured");
		}
	}

	public static void CreateAndPopulateOperatorTable(String databaseURL, String dbName, String user, String password)
			throws Exception {
		/*
		 * this function will create OPERATOR_DETAILS table which will have specified
		 * ranges divided in operators and regions.
		 */
		String[] regions = new String[] { "UP", "HP", "MP", "Punjab", "Haryana" };
		// creating a string of all the regions of the operator

		try (Connection con = DriverManager.getConnection(databaseURL + dbName + "", user, password);) {
			Statement statement = con.createStatement();
			// creating table1 ie OPERATOR_DETAILS which will have the ranges of the
			// operators and their regions.
			String sql = "CREATE TABLE OPERATOR_DETAILS " + "(ranges INTEGER , " + " operator VARCHAR(255), "
					+ " region VARCHAR(255)) ";

			statement.executeUpdate(sql);

			PreparedStatement ps = con.prepareStatement("insert into OPERATOR_DETAILS values(?,?,?)");
			// creating table to simplify the ranges of phone numbers and their operators.
			// specifying range for regions of Airtel operator
			int airtelRange = 98720;
			for (int i = 0; i < regions.length; i++) {
				ps.setInt(1, airtelRange);
				ps.setString(2, "Airtel");
				ps.setString(3, regions[i]);
				ps.addBatch();
				
			}
			// specifying range for regions of Idea operator
			int ideaRange = 98140;
			for (int i = 0; i < regions.length; i++) {
				ps.setInt(1, ideaRange);
				ps.setString(2, "Idea");
				ps.setString(3, regions[i]);
				ps.addBatch();
				ideaRange += 2;
			}

			// specifying range for regions of Vodafone operator
			int vodaRange = 70180;
			for (int i = 0; i < regions.length; i++) {
				ps.setInt(1, vodaRange);
				ps.setString(2, "Vodafone");
				ps.setString(3, regions[i]);
				ps.addBatch();
				vodaRange += 2;
			}

			// specifying range for regions of Jio operator
			int jioRange = 94590;

			for (int i = 0; i < regions.length; i++) {
				ps.setInt(1, jioRange);
				ps.setString(2, "Jio");
				ps.setString(3, regions[i]);
				ps.addBatch();
				jioRange += 2;
			}

			ps.executeBatch();
			CreateAndPopulateMSGTable(databaseURL, dbName, user, password);
			// calling second table function for saving message details.

		} catch (Exception e) {
			throw new Exception("some error with operator table" + e);
		}
	}

	public static void CreateAndPopulateMSGTable(String databaseURL, String dbName, String user, String password)
			throws Exception {
		/*
		 * second table function create MSG_DETAILS table which will have info about msg
		 * sent and received.
		 */
		try (Connection con = DriverManager.getConnection(databaseURL + dbName + "", user, password);) {
			Statement statement = con.createStatement();
			// query to create table.
			String sql1 = "CREATE TABLE MSG_DETAILS " + "(sentFrom BIGINT not NULL, " + " sentTo BIGINT not NULL, "
					+ " message VARCHAR(255) not NULL," + " fromOperator VARCHAR(255), " + " toOperator VARCHAR(255), "
					+ " FromRegion VARCHAR(255), " + " toRegion VARCHAR(255), " + " sentTime VARCHAR(255), "
					+ " recievedTime VARCHAR(255) ," + " deliveryStatus VARCHAR(255)" + ")";

			statement.execute(sql1);
			// inserting values in the second table.

			Scanner ob = new Scanner(System.in);
			System.out.println("Enter the number of entries you'll like to make.");
			int entryCount = ob.nextInt();
			if (entryCount > 0) {
				for (int i = 0; i < entryCount; i++) {
					System.out.println("enter the mobile number which sent the messages");
					long sentFrom = ob.nextLong();

					while (sentFrom > 99999) { // extracting the first 5 digits to fetch the range
						sentFrom = sentFrom / 10;
					}
					int SentRange = (int) sentFrom;// range of number from which the msg was sent

					// getting details of the operator and region of the receiver from the operator
					// table
					ResultSet rs = statement.executeQuery(
							"select operators,region from mobile_operators where ranges=" + SentRange + "");
					rs.next();
					String fromOperator = rs.getString(1);
					String FromRegion = rs.getString(2);

					System.out.println("enter the mobile number to which the messages was sent ");
					long sentTo = ob.nextLong();

					while (sentFrom > 99999) { // extracting the first 5 digits to fetch the range
						sentTo = sentTo / 10;
					}
					int ToRange = (int) sentTo;// range of number from which the msg was sent

					// getting details of the operator and region of the receiver from the operator
					// table
					ResultSet rs1 = statement
							.executeQuery("select operators,region from mobile_operators where ranges=" + ToRange + "");
					rs1.next();
					String toOperator = rs1.getString(1);
					String toRegion = rs1.getString(2);
					System.out.println("enter the message.");
					String message = ob.nextLine();

					System.out.println("enter the time of the sending msg.");
					String sentTime = ob.nextLine();

					System.out.println("enter the time of recieving msg.");
					String ReceivedTime = ob.nextLine();

					System.out.println("enter the delivery status");
					String deliveryStatus = ob.nextLine();

					PreparedStatement ps = con.prepareStatement(
							"insert into messages values(?,?,?,?,?,?,?,?,?,?)");

					ps.setLong(1, sentFrom);
					ps.setLong(2, sentTo);
					ps.setString(3, message);
					ps.setString(4, fromOperator);
					ps.setString(5, toOperator);
					ps.setString(6, FromRegion);
					ps.setString(7, toRegion);
					ps.setString(8, sentTime);
					ps.setString(9, ReceivedTime);
					ps.setString(10, deliveryStatus);
				}

			} else {
				log.info("no entries made to the msg table.");
			}
		} catch (Exception e) {
			throw new Exception("some error with msg table.");
		}
	}
	/*
	 * String newsql =
	 * "INSERT into MSG_DETAILS values(9872900001,981490000,'Hey',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 7 NOV,2017','12:56; 7 NOV,2017','Received')"
	 * ; statement.addBatch(newsql); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872900301,9814000000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 7 NOV,2017','12:56; 7 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9867900301,9814500000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98679 ),(SELECT operator from OPERATOR_DETAILS where ranges=98145),(SELECT region from OPERATOR_DETAILS where ranges=98679 ),(SELECT region from OPERATOR_DETAILS where ranges=98145),'12:56; 7 NOV,2017','11:10; 7 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9832777777,9814000000,'??',(SELECT operator from OPERATOR_DETAILS where ranges=98327 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98327 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 6 NOV,2017','12:56; 7 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814200000,9832777777,'HBD!',(SELECT operator from OPERATOR_DETAILS where ranges=98142 ),(SELECT operator from OPERATOR_DETAILS where ranges=98327),(SELECT region from OPERATOR_DETAILS where ranges=98142 ),(SELECT region from OPERATOR_DETAILS where ranges=98327),'12:56; 17 NOV,2017','12:56; 17 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872900301,9832777777,'HNY!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98327),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98327),'12:56; 8 NOV,2017','12:56; 17 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814078900,9814000000,'HI!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 17 NOV,2017','12:56; 17 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814078900,9814000000,'HBD!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 26 NOV,2017','12:56; 27 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872900301,9814000000,'HNY!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 3 NOV,2017','12:56; 7 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872900301,9814200000,'BYE!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98142),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98142),'12:56; 4 NOV,2017','12:56; 7 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872900301,9814000000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 11 NOV,2017','12:56; 17 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9867900301,9814500000,'BYE!',(SELECT operator from OPERATOR_DETAILS where ranges=98679 ),(SELECT operator from OPERATOR_DETAILS where ranges=98145),(SELECT region from OPERATOR_DETAILS where ranges=98679 ),(SELECT region from OPERATOR_DETAILS where ranges=98145),'12:56; 13 NOV,2017','12:56; 13 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9832777777,9814000000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98327 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98327 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 13 NOV,2017','12:56; 13 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814200000,9832777777,'NO!',(SELECT operator from OPERATOR_DETAILS where ranges=98142 ),(SELECT operator from OPERATOR_DETAILS where ranges=98327),(SELECT region from OPERATOR_DETAILS where ranges=98142 ),(SELECT region from OPERATOR_DETAILS where ranges=98327),'12:56; 6 NOV,2017','12:56; 7 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872900301,9832777777,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98327),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98327),'12:56; 12 NOV,2017','12:56; 14 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814078900,9814000000,'YES!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 4 NOV,2017','12:56; 4 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814078900,9814000000,'OKAY!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98109),'12:56; 9 NOV,2017','12:56; 10 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872900301,9814000000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 17 NOV,2017','12:56; 28 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872600301,9814200000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98726 ),(SELECT operator from OPERATOR_DETAILS where ranges=98142),(SELECT region from OPERATOR_DETAILS where ranges=98726 ),(SELECT region from OPERATOR_DETAILS where ranges=98142),'12:56; 17 NOV,2017','','NOT recieved')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814678900,9814000000,'OKAY!',(SELECT operator from OPERATOR_DETAILS where ranges=98146 ),(SELECT operator from OPERATOR_DETAILS where ranges=98140),(SELECT region from OPERATOR_DETAILS where ranges=98146 ),(SELECT region from OPERATOR_DETAILS where ranges=98140),'12:56; 9 NOV,2017','','NOT recieved')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872600301,9814900000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98726 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98726 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 17 NOV,2017','','NOT recieved')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872600301,9814200000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98726 ),(SELECT operator from OPERATOR_DETAILS where ranges=98142),(SELECT region from OPERATOR_DETAILS where ranges=98726 ),(SELECT region from OPERATOR_DETAILS where ranges=98142),'12:56; 17 NOV,2017','','NOT recieved')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814660000,9872900301,'NO!',(SELECT operator from OPERATOR_DETAILS where ranges=98146 ),(SELECT operator from OPERATOR_DETAILS where ranges=98729),(SELECT region from OPERATOR_DETAILS where ranges=98146 ),(SELECT region from OPERATOR_DETAILS where ranges=98729),'12:56; 6 NOV,2017','12:56; 7 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872760302,9872900301,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98726 ),(SELECT operator from OPERATOR_DETAILS where ranges=98729),(SELECT region from OPERATOR_DETAILS where ranges=98726 ),(SELECT region from OPERATOR_DETAILS where ranges=98729),'12:56; 12 NOV,2017','12:56; 14 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814078900,9872900301,'YES!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98729),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98729),'12:56; 4 NOV,2017','12:56; 4 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9814078900,9872900301,'OKAY!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98729),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98729),'12:56; 9 NOV,2017','12:56; 10 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872900301,9878691266,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98786),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98786),'12:56; 17 NOV,2017','12:56; 28 NOV,2017','Received')"
	 * ); statement.addBatch(
	 * "INSERT into MSG_DETAILS values(9872900301,9878691290,'Hii!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98786),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98786),'12:56; 17 NOV,2017','','NOT recieved')"
	 * ); statement.executeBatch();
	 */

	public static void finalQueryOutputs(String dbURL, String dbName, String user, String password)
			throws SQLException {
		Connection con = DriverManager.getConnection(dbURL + dbName + "", user, password);
		Statement statement = con.createStatement();
		/*
		 * Scanner ob= new Scanner (System.in);
		 * System.out.println("enter the mobile number which sent the messages");
		 * BigInteger phone1= ob.nextBigInteger(); System.out.
		 * println("enter the mobile number to which the messages where sent.");
		 * BigInteger phone2= ob.nextBigInteger(); if(phone1!=null) { Boolean
		 * check="SELECT MSG_DETAILS FROM to WHERE EXISTS
		 */
		/*
		 * long phone1= 9872900301; long phone2= 9832777777;
		 */
		log.info("print all messages sent from *** number To any number.");
		String query1 = "SELECT message from MSG_DETAILS where sentFrom = 9872900301";
		ResultSet result1 = statement.executeQuery(query1);
		while (result1.next()) {
			log.info(result1.getString("message"));
		}
		log.info("print all messages received by * from any number");
		String query2 = "SELECT message from MSG_DETAILS where sentTo = 9872900301";
		ResultSet result2 = statement.executeQuery(query2);
		while (result2.next()) {
			log.info(result2.getString("message"));
		}
		log.info("print all messages sent from XXXX to YYYY");
		String query3 = " SELECT message from MSG_DETAILS where sentFrom = 9872900301 and sentTo = 9832777777";
		ResultSet result3 = statement.executeQuery(query3);
		while (result3.next()) {
			log.info(result3.getString("message"));
		}
		log.info("print all messages received by * from Punjab number");
		String query4 = "SELECT message from MSG_DETAILS where sentTo = 9872900301 and fromRegion = 'Punjab' ";
		ResultSet result4 = statement.executeQuery(query4);
		while (result4.next()) {
			log.info(result4.getString("message"));
		}
		log.info("print all messages received by * from Airtel Punjab number");
		String query5 = "SELECT message from MSG_DETAILS where sentTo= 9872900301 and fromOperator= 'Airtel' and fromRegion = 'Punjab'";
		ResultSet result5 = statement.executeQuery(query5);
		while (result5.next()) {
			log.info(result5.getString("message"));
		}
		log.info(
				"print all messages received from 98786912** ( here * mean it can by any two digit, i.e. messages from 9878691291,92,93,94,95 etc..) by **.");
		String query6 = "SELECT message from MSG_DETAILS where sentTo LIKE '98786912__' and sentFrom = 9872900301";
		ResultSet result6 = statement.executeQuery(query6);
		while (result6.next()) {
			log.info(result6.getString("message"));
		}
		log.info("print all messages those were sent from Punjab number but FAILED");
		String query7 = "SELECT message from MSG_DETAILS where fromRegion = 'Punjab' and deliveryStatus ='NOT recieved'";
		ResultSet result7 = statement.executeQuery(query7);
		while (result7.next()) {
			log.info(result7.getString("message"));
		}
	}

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
			System.out.println(databaseURL);
			String user = ReadConfigFile.getResources("user");
			System.out.println(user);

			String password = ReadConfigFile.getResources("password");
			System.out.println(password);

			String dbName = "NetworkOperatorDatabase";

			checkDatabase(databaseURL, dbName, user, password);
			finalQueryOutputs(databaseURL, dbName, user, password);

		} catch (Exception e) {
			log.fatal("some error occured." + e);
		}

	}
}
