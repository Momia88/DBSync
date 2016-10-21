package com.coretronic.hpbu;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.activation.FileDataSource;
import javax.ws.rs.core.MediaType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.sun.javafx.css.Rule;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.org.apache.xml.internal.security.Init;

import sun.awt.image.PNGImageDecoder.Chromaticities;

public class DBManager {

	private static Logger logger = LogManager.getLogger(DBManager.class.getName());
	private String KEY_API_KEY = "ApiKey";
	private String apiKey = "uaEPLpCWk4Jmp9nxg83YG0pW0B780wqcU0O1aXPVI04=";
	private String KEY_API_NAME = "ApiName";
	private String apiName = "CreateSiteState";
	private String dataFormatStr = "yyyyMMddHHmmss";
	private Gson gson = new Gson();
	private String PRIVATE_KEY_DATE = "P_DATE";
	private String PRIVATE_KEY_TIME = "TIME";
	private String PRIVATE_KEY_MODEL_NO = "MODEL_NO";
	private String PRIVATE_KEY_PRODUCT_SN = "PRODUCT_SN";

	public List<ChromaObj> getDuration(String url, Calendar calendar, int durationHour, int overTime) {

		Calendar sCalendar = Calendar.getInstance();
		sCalendar.setTime(calendar.getTime());
		DateFormat dateFormat = new SimpleDateFormat(dataFormatStr);
		String startTime = dateFormat.format(sCalendar.getTime());
		logger.info("Sync Date: " + startTime);
		sCalendar.add(Calendar.HOUR, durationHour);
		sCalendar.add(Calendar.SECOND, overTime);
		String endTime = dateFormat.format(sCalendar.getTime());

		String webUrl = url + "?queryStartDateTime=" + startTime + "&queryEndDateTime=" + endTime;
		System.out.println(webUrl);
		Client client = Client.create();
		ClientResponse response = client.resource(webUrl).accept(MediaType.APPLICATION_JSON).header(KEY_API_KEY, apiKey)
				.header(KEY_API_NAME, apiName).get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		}

		String output = response.getEntity(String.class);
		Gson gson = new Gson();
		List<ChromaObj> chromaList = gson.fromJson(output, new TypeToken<List<ChromaObj>>() {
		}.getType());

		return chromaList;
	}

	public int saveToDB(Connection connect, List<ChromaObj> chromaList) {
		int status = 0;
		String columnStr = "";
		String valueStr = "";
		HashMap<String, String> keypart = new HashMap<>();

		for (ChromaObj chromaObj : chromaList) {
			try {
				JSONObject jsonObject = new JSONObject(gson.toJson(chromaObj));
				columnStr = getColumnNames(jsonObject);
				valueStr = getValues(jsonObject);

				String sqlStr = "INSERT INTO SFCS_RUNCARD_CHROMA_VIEW (" + columnStr + ") VALUES (" + valueStr + ")";

				if (checkRecord(connect, jsonObject) == 0) {
					// Load the MySQL driver, each DB has its own driver
					PreparedStatement preparedStatement = connect.prepareStatement(sqlStr);
					status = preparedStatement.executeUpdate();
					if (status > 0) {
						logger.info("Sync Success!!");
					}
				}
			} catch (Exception e) {
				logger.error("Error", e);
			}

		}
		return status;
	}

	// Get sql insert column names
	public String getColumnNames(JSONObject jsonObject) {
		String str = "";
		try {
			Iterator fields = jsonObject.keys();
			while (fields.hasNext()) {
				String key = (String) fields.next();
				if (fields.hasNext()) {
					str += key + ",";
				} else {
					str += key;
				}
			}
		} catch (Exception e) {
			logger.error("Error", e);
		}
		return str;

	}

	// Get sql insert values
	private String getValues(JSONObject jsonObject) {
		String str = "";
		try {
			Iterator fields = jsonObject.keys();
			while (fields.hasNext()) {
				String key = (String) fields.next();
				String value = jsonObject.getString(key);
				str += "'" + value + "'";
				if (fields.hasNext()) {
					str += ",";
				}
			}
		} catch (Exception e) {
			logger.error("Error", e);
		}
		return str;
	}

	// Check record exist or not
	private int checkRecord(Connection connect, JSONObject jsonObject) {
		ResultSet resultSet;
		try {

			String pDate = jsonObject.getString(PRIVATE_KEY_DATE);
			String pTime = jsonObject.getString(PRIVATE_KEY_TIME);
			String pModel = jsonObject.getString(PRIVATE_KEY_MODEL_NO);
			String pProductSN = jsonObject.getString(PRIVATE_KEY_PRODUCT_SN);

			String sqlStr = "select count(1) from SFCS_RUNCARD_CHROMA_VIEW where P_DATE='" + pDate + "' and TIME='"
					+ pTime + "' and MODEL_NO='" + pModel + "' and PRODUCT_SN='" + pProductSN + "'";

			// Load the MySQL driver, each DB has its own driver
			PreparedStatement preparedStatement = connect.prepareStatement(sqlStr);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				if (resultSet.getInt(1) > 0) {
					logger.info("Exist: " + "P_DATE='" + pDate + "',TIME='" + pTime + "',MODEL_NO='" + pModel
							+ "',PRODUCT_SN='" + pProductSN + "'");
				}
				return resultSet.getInt(1);
			}
		} catch (Exception e) {
			logger.error("Error", e);
		}
		return 0;
	}
	
	public int[] strToIntArray(String str){
		int[] arr = new int[6];
		arr[0] = Integer.valueOf(str.substring(0, 4));
		arr[1] = Integer.valueOf(str.substring(4, 6));
		arr[2] = Integer.valueOf(str.substring(6, 8));
		arr[3] = Integer.valueOf(str.substring(8, 10));
		arr[4] = Integer.valueOf(str.substring(10, 12));
		arr[5] = Integer.valueOf(str.substring(12));
		return arr;
	}
	
}
