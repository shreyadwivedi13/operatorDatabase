package assignment3.jdbc;

import java.sql.*;
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
	//taking default values of url, username and password in case config file fails to read.
	static final String DB_URL = "jdbc:mysql://localhost:3306/";
	static final String USER = "root";
	static final String PASSWORD = "root";

	public static final Logger log = LogManager.getLogger(MobileOperatorDatabase.class.getName());
	


	public static void checkDatabase(String databaseURL, String dbName, String user, String password) throws SQLException {
		/*checking if the database already exists or not, if not then creating a database with required dbName.*/
		ResultSet rs = null;
		Connection con= null;
		con = DriverManager.getConnection(databaseURL, user, password);
		if (con != null) {
			
			log.info("Checking if a database exists");
			rs = con.getMetaData().getCatalogs();
			boolean dbExists = true;
			while (rs.next()) {
				String catalogs = rs.getString(1);
				
				if (dbName.equals(catalogs)) {
					dbExists = true;
					break;
				} else {
					dbExists = false;
				}

			}
			log.info("Database exist : " + dbExists);
			if(dbExists== true) {
				Statement statement = con.createStatement();
				String sql = "DROP DATABASE DBNAME";
				statement.executeUpdate(sql);
			}
			if (dbExists == false) {
				log.info("no database found with this name so creating new database");
				Statement statement = con.createStatement();
				String sql = "CREATE DATABASE " + dbName;
				statement.executeUpdate(sql);
				log.info("Database with name " + dbName + " created.");
			}

		}

	}

	public static void CreateAndPopulateTables(String databaseURL, String dbName, String user, String password)
			throws SQLException {
		/*
		 * this function will both the tables 1)create OPERATOR_DETAILS table which will
		 * have specified ranges divided in operators and regions. 2)create MSG_DETAILS
		 * table which will have info about msg sent and recieved.
		 */
		String[] regions = new String[] { "UP", "HP", "MP", "Punjab", "Haryana" };
		// creating a string of all the regions of the operator

		Connection con = DriverManager.getConnection(databaseURL + dbName + "", user, password);
		Statement statement = con.createStatement();
		//creating table1 ie OPERATOR_DETAILS which will have the ranges of the operators and their regions. 
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
			airtelRange += 2;
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

		// specifying range for regions of Voda operator
		int vodaRange = 70180;
		for (int i = 0; i < regions.length; i++) {
			ps.setInt(1, vodaRange);
			ps.setString(2, "Voda");
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
		// creating second table for saving message details.
		String sql1 = "CREATE TABLE MSG_DETAILS " + "(from BIGINT not NULL, " + " to BIGINT not NULL, " + " message VARCHAR(225) not NULL," + " fromOperator VARCHAR(255), " + " toOperator VARCHAR(255), " + " FromRegion VARCHAR(255), " + " toRegion VARCHAR(255), " + " sentTime VARCHAR(255), "+ " recievedTime VARCHAR(255) ," + " deliveryStatus VARCHAR(255)" + ")";

		statement.execute(sql1);
		//inserting values in the second table.
		String newsql = "INSERT into MSG_DETAILS values(9872900001,981490000,'Hey',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 7 NOV,2017','12:56; 7 NOV,2017','Received')";
		statement.addBatch(newsql);
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,981400000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 7 NOV,2017','12:56; 7 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9867900301,981450000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98679 ),(SELECT operator from OPERATOR_DETAILS where ranges=98145),(SELECT region from OPERATOR_DETAILS where ranges=98679 ),(SELECT region from OPERATOR_DETAILS where ranges=98145),'12:56; 7 NOV,2017','11:10; 7 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9832777777,981400000,'??',(SELECT operator from OPERATOR_DETAILS where ranges=98327 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98327 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 6 NOV,2017','12:56; 7 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9814200000,983277777,'HBD!',(SELECT operator from OPERATOR_DETAILS where ranges=98142 ),(SELECT operator from OPERATOR_DETAILS where ranges=98327),(SELECT region from OPERATOR_DETAILS where ranges=98142 ),(SELECT region from OPERATOR_DETAILS where ranges=98327),'12:56; 17 NOV,2017','12:56; 17 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,983277777,'HNY!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98327),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98327),'12:56; 8 NOV,2017','12:56; 17 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9814078900,981400000,'HI!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 17 NOV,2017','12:56; 17 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9814078900,981400000,'HBD!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 26 NOV,2017','12:56; 27 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,981400000,'HNY!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 3 NOV,2017','12:56; 7 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,981420000,'BYE!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98142),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98142),'12:56; 4 NOV,2017','12:56; 7 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,981400000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 11 NOV,2017','12:56; 17 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9867900301,981450000,'BYE!',(SELECT operator from OPERATOR_DETAILS where ranges=98679 ),(SELECT operator from OPERATOR_DETAILS where ranges=98145),(SELECT region from OPERATOR_DETAILS where ranges=98679 ),(SELECT region from OPERATOR_DETAILS where ranges=98145),'12:56; 13 NOV,2017','12:56; 13 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9832777777,981400000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98327 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98327 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 13 NOV,2017','12:56; 13 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9814200000,983277777,'NO!',(SELECT operator from OPERATOR_DETAILS where ranges=98142 ),(SELECT operator from OPERATOR_DETAILS where ranges=98327),(SELECT region from OPERATOR_DETAILS where ranges=98142 ),(SELECT region from OPERATOR_DETAILS where ranges=98327),'12:56; 6 NOV,2017','12:56; 7 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,983277777,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98327),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98327),'12:56; 12 NOV,2017','12:56; 14 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9814078900,981400000,'YES!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 4 NOV,2017','12:56; 4 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9814078900,981400000,'OKAY!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 9 NOV,2017','12:56; 10 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,981400000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 17 NOV,2017','12:56; 28 NOV,2017','Received')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,981420000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98142),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98142),'12:56; 17 NOV,2017','','NOT recieved')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9814078900,981400000,'OKAY!',(SELECT operator from OPERATOR_DETAILS where ranges=98140 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98140 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 9 NOV,2017','','NOT recieved')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,981400000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98149),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98149),'12:56; 17 NOV,2017','','NOT recieved')");
		statement.addBatch(
				"INSERT into MSG_DETAILS values(9872900301,981420000,'Hey!',(SELECT operator from OPERATOR_DETAILS where ranges=98729 ),(SELECT operator from OPERATOR_DETAILS where ranges=98142),(SELECT region from OPERATOR_DETAILS where ranges=98729 ),(SELECT region from OPERATOR_DETAILS where ranges=98142),'12:56; 17 NOV,2017','','NOT recieved')");

		statement.executeBatch();
	}

	public static void finalQueryOutputs(String dbURL, String dbName, String user, String password)
			throws SQLException {
		Connection con = DriverManager.getConnection(dbURL + dbName +"", user, password );
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
		String query1 = "SELECT message from MSG_DETAILS where From = 9872900301";
		ResultSet result1 = statement.executeQuery(query1);
		while (result1.next()) {
			log.info(result1.getString("message"));
		}
		log.info("print all messages received by * from any number");
		String query2 = "SELECT message from MSG_DETAILS where To = 9872900301";
		ResultSet result2 = statement.executeQuery(query2);
		while (result2.next()) {
			log.info(result2.getString("message"));
		}
		log.info("print all messages sent from XXXX to YYYY");
		String query3 = " SELECT message from MSG_DETAILS where From = 9872900301 and To = 9832777777";
		ResultSet result3 = statement.executeQuery(query3);
		while (result3.next()) {
			log.info(result3.getString("message"));
		}
		log.info("print all messages received by * from Punjab number");
		String query4 = "SELECT message from MSG_DETAILS where To = 9872900301 and fromRegion = 'Punjab' ";
		ResultSet result4 = statement.executeQuery(query4);
		while (result4.next()) {
			log.info(result4.getString("message"));
		}
		log.info("print all messages received by * from Airtel Punjab number");
		String query5 = "SELECT message from MSG_DETAILS where To= 9872900301 and fromOperator= 'Airtel' and fromRegion = 'Punjab'";
		ResultSet result5 = statement.executeQuery(query5);
		while (result5.next()) {
			log.info(result5.getString("message"));
		}
		log.info("print all messages received by * from Airtel Punjab number");
		String query6 = "SELECT message from MSG_DETAILS where To LIKE '98786912__' and From = 9872900301";
		ResultSet result6 = statement.executeQuery(query6);
		while (result6.next()) {
			log.info(result6.getString("message"));
		}
		log.info("print all messages those were sent from Punjab number but FAILED");
		String query7 = "SELECT message from MSG_DETAILS where fromRegion = 'Punjab' and deliveryStatus ='NOT recieve'";
		ResultSet result7 = statement.executeQuery(query7);
		while (result7.next()) {
			log.info(result7.getString("message"));
		}
	}
	public static void resourceIntializer() throws Exception {
		ReadConfigFile.getFile();
	}

	public static void main(String[] args) throws Exception {
		// Class.forName("com.mysql.jdbc.Driver");
		resourceIntializer();

		String user = ReadConfigFile.getResources("user");
		if (user == null) {
			user = USER;
		}
		String password = ReadConfigFile.getResources("password");
		if (password == null) {
			password = PASSWORD;
		}
		String databaseURL = ReadConfigFile.getResources("databaseURL");
		if (databaseURL == null) {
			databaseURL = DB_URL;
		}
		String dbName = "m5";
		checkDatabase(databaseURL, dbName, user, password);

		CreateAndPopulateTables(databaseURL, dbName, user, password);
		finalQueryOutputs(databaseURL, dbName, user, password);

	}

}
