package com.github.arikastarvo.comet.reference;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

public class SQLiteReferenceDataSourceFactory {
	
	private static Map<String, DataSource> dataSources = new HashMap<String, DataSource>();
	
	private static final String DEFAULT_DS = "__DEFAULT__";
	
	public static DataSource createDataSource(Properties properties) throws Exception {
		
		// return existing
		if(properties.containsKey("monitor") && dataSources.containsKey(properties.get("monitor"))) {
			return dataSources.get(properties.get("monitor"));
		
		} else if(properties.containsKey("monitor")) {
			BasicDataSource ds = BasicDataSourceFactory.createDataSource(properties);
			dataSources.put((String)properties.get("monitor"), ds);
			return ds;
		
		} else if(dataSources.containsKey(DEFAULT_DS)) {
			return dataSources.get(DEFAULT_DS);
		
		} else {
			BasicDataSource ds = BasicDataSourceFactory.createDataSource(properties);
			dataSources.put(DEFAULT_DS, ds);
			return ds;			
		}
	}
}
