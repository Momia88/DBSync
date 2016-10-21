package com.coretronic.hpbu;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SyncMain {

	private static int durationHour = 1;
	private static String serverPath = "http://10.2.1.38/log/api";
	private static String methodName = "/CreateSiteState";
	private static Logger logger = LogManager.getLogger(SyncMain.class.getName());
	private static String jdbcStr = "jdbc:mysql://192.168.3.88/chroma_log?autoReconnect=true&useSSL=false";
//	private static String jdbcStr = "jdbc:mysql://localhost/chroma_log?autoReconnect=true&useSSL=false";
	private static String user = "chroma";
	private static String passwd = "cindy7377";
	private static int overTime = 10;

	public static void main(String[] args) {

		logger.info("app start !!");

		// DB connection setup
		Connection connect = null;
		DBManager queryManager = new DBManager();

		// init DateTime
		Calendar stopCalendar = Calendar.getInstance();
		Calendar startCalendar = Calendar.getInstance();
		if (args.length == 0) {
			startCalendar.add(Calendar.HOUR, -1);
		} else if (args.length == 1) {
			int[] startDate = queryManager.strToIntArray(args[0]);
			startCalendar.set(startDate[0], startDate[1], startDate[2], startDate[3], startDate[4], startDate[5]);
		} else {
			if (!args[0].matches("[0-9]{14}") || !args[1].matches("[0-9]{14}")) {
				logger.error("Error", "input date formate error!!");
				return;
			}
			int[] startDate = queryManager.strToIntArray(args[0]);
			int[] endDate = queryManager.strToIntArray(args[1]);

			startCalendar.set(startDate[0], startDate[1], startDate[2], startDate[3], startDate[4], startDate[5]);
			stopCalendar.set(endDate[0], endDate[1], endDate[2], endDate[3], endDate[4], endDate[5]);
		}

		logger.info("Date: " + startCalendar.getTime() + " / " + stopCalendar.getTime());
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection(jdbcStr, user, passwd);
			if (connect == null) {
				logger.error("Error", "DB Connection Fails!!");
				return;
			}

			String webUrl = serverPath + methodName;

			while (startCalendar.getTime().before(stopCalendar.getTime())) {
				try {
					double runStart = Calendar.getInstance().getTimeInMillis();
					// get data from CPC
					List<ChromaObj> chromaList = queryManager.getDuration(webUrl, startCalendar, durationHour, overTime);
					startCalendar.add(Calendar.HOUR, durationHour);
					// save record
					queryManager.saveToDB(connect, chromaList);
					double second = (Calendar.getInstance().getTimeInMillis() - runStart) / 1000.0;
					logger.info("Run Time: " + second);
				} catch (Exception e) {
					logger.error("Error", e);
				}
			}
			logger.info("Sync Finish !!");
			System.out.println("Sync Finish !!");
		} catch (SQLException e) {
			logger.error("Error", e);
		} catch (ClassNotFoundException e) {
			logger.error("Error", e);
		} finally {
			try {
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e) {
				logger.error("Error", e);
			}
		}
	}

}
