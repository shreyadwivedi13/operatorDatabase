package assignment3.jdbc;
	/*
	 * this file reads the configuration file.
	 * author @shreya.dwivedi
	 */
	import org.apache.logging.log4j.LogManager;
	import org.apache.logging.log4j.Logger;
	import java.io.FileInputStream;
	import java.util.Properties;

	public class ReadConfigFile {

		static Logger log = LogManager.getLogger(MobileOperatorDatabase.class.getName());
		public static Properties property = new Properties();

		// the following function returns the properties instance which can be used
		// to access the configuration files
		public static void getFile()throws Exception {
			// try with resources to open and access the configuration file
			try (FileInputStream propertyfile = new FileInputStream("./src/main/resources/configFile.properties");) {
				property.load(propertyfile);
			}
			// logs the error and exists the system in case of empty config file
			catch (Exception e) {
				throw new Exception("Issue with config.property file.");
				
			}
		}
		public static String getResources(String key) {
			return property.getProperty(key);
		}

	}



