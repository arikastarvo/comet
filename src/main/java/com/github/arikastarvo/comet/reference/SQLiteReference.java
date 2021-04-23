package com.github.arikastarvo.comet.reference;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.arikastarvo.comet.input.ReferenceInput;

public class SQLiteReference implements Reference, ReferenceReloadCallback {
	
	//private final String urlBase = "jdbc:sqlite:";
	private final String urlBase = "";
	
	String name;

	String driverClassName = "org.sqlite.JDBC";
	//String url = urlBase + "/tmp/sample.db";
	String url = urlBase + ":memory:";
	String validationQuery = "select 1";
	Integer LRUCache;
	Integer cacheExpiryMaxAge;
	Integer cacheExpiryPurgeInterval;
	
	private boolean inBatchMode = false;
	private boolean doTruncate = false;
	
	private int busy_timeout = 10000;
	
	boolean dropOnInit = false;

	Logger log = LogManager.getLogger(SQLiteReference.class);
	
	private Connection connection = null;
	
	private DataSource dataSource; // = new BasicDataSource();
	
	Map<String, Map<String, String>> fields = new HashMap<String, Map<String,String>>();
	
	@SuppressWarnings("unused")
	private SQLiteReference() {
	}
	
	/** 
	 * 
	 * TODO: this connection and the connection used in monitorruntime, aren't shared.. so fuck this shit
	 * 
	 * @param name
	 */
	public SQLiteReference(String name) {
		this.name = name;
		this.initializeConnection();
	}
	
	public SQLiteReference(String name, String url) {
		this.name = name;
		this.url = urlBase + url;
		this.initializeConnection();
	}
	
	private void initializeConnection() {
		try {
			//dataSource.setUrl(getUrl());
			//dataSource.setDriverClassName(getDriverClassName());
			//dataSource.prop
			//dataSource.setpr
			//dataSource.setMinIdle(5);
			//dataSource.setMaxIdle(10);
			//dataSource.setMaxOpenPreparedStatements(100);
			Properties props = getProperties();
			props.put("monitor", this.name);
			
			//this.dataSource = SQLiteReferenceDataSourceFactory.createDataSource(props);
			//this.connection = (SQLiteConnection)DriverManager.getConnection(this.url);
			//((SQLiteConnection)this.connection).setBusyTimeout(busy_timeout);
			
			this.dataSource = BasicDataSourceFactory.createDataSource(props);
			this.connection = this.dataSource.getConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getName() {
		return name;
	}
	
	public String getDriverClassName() {
		return driverClassName;
	}

	public String getUrl() {
		return url;
	}

	public Properties getProperties() {
		Properties props = new Properties();
		props.put("validationQuery", validationQuery);
		props.put("busy_timeout", busy_timeout);
		
		props.put("url", getUrl());
		props.put("driverClassName", getDriverClassName());
		return props;
	}

	public Integer getLRUCacheSize() {
		return LRUCache;
	}

	public Integer getCacheExpiryMaxAge() {
		return cacheExpiryMaxAge;
	}

	public Integer getCacheExpiryPurgeInterval() {
		return cacheExpiryPurgeInterval;
	}

	public boolean dropOnInit() {
		return dropOnInit;
	}

	
	public void setFields(Map<String, Map<String, String>> fields) {
		this.fields = fields;
	}

	public void setLRUCacheSize(int cacheSize) {
		this.LRUCache = cacheSize;
	}

	public void setCacheExpiryMaxAge(int cacheMaxAge) {
		this.cacheExpiryMaxAge = cacheMaxAge;
	}

	public void setCacheExpiryPurgeInterval(int cachePurgeInterval) {
		this.cacheExpiryPurgeInterval = cachePurgeInterval;
	}
	
	public void setDropOnInit(boolean dropOnInit) {
		this.dropOnInit = dropOnInit;
	}

	private List<String> eventTypes = new ArrayList<String>();
	
	private static String generateCreateTableClause(String eventType, Map<String, String> fields) {
		String createFields = fields.entrySet().stream().map((Entry<String, String> entry) -> entry.getKey() + " " + entry.getValue()).collect(Collectors.joining(", "));
		//String clause = String.format("create table IF NOT EXISTS %s (%s)", eventType, createFields);
		String clause = String.format("create table if not exists %s (%s)", eventType, createFields);
		return clause;
	}
	
	private static String generateInsertClause(String eventType, Map<String, Object> data, Map<String, String> allowedFields) {
		String insertFields = data.entrySet().stream()
				.filter((Entry<String, Object> entry) -> allowedFields.containsKey(entry.getKey()))
				.map((Entry<String, Object> entry) -> entry.getKey())
				.collect(Collectors.joining(", "));
		
		/// CONVERT THIS TO PREPARED STATEMENTS !!
		String insertValues = data.entrySet().stream()
				.filter((Entry<String, Object> entry) -> allowedFields.containsKey(entry.getKey()))
				.map((Entry<String, Object> entry) -> {
					if(entry.getValue() instanceof Integer) {
						return ((Integer)entry.getValue()).toString();
					} else if(entry.getValue() instanceof Long) {
						return ((Long)entry.getValue()).toString();
					} else {
						return "'" + (String)entry.getValue() + "'";
					}
				})
				.collect(Collectors.joining(", "));
		String clause = String.format("insert into %s (%s) values (%s)", eventType, insertFields, insertValues);
		return clause;
	}
	
	Statement batchStatement;
	
	
	public void reloadInit() {
		try {
			batchStatement = connection.createStatement();
			//batchStatement.addBatch("PRAGMA writable_schema = 1");
			//batchStatement.addBatch("delete from sqlite_master where type in ('table', 'index', 'trigger')");
			//batchStatement.addBatch("PRAGMA writable_schema = 0");
			//batchStatement.addBatch("VACUUM");
			eventTypes.clear();
			this.inBatchMode = true;
		} catch (SQLException e) {
			doTruncate = true;
			log.error("could not initialize batch mode : {}", e.getMessage());
		}
	}
	
	public void reloadFinalize() {
		try {

			batchStatement.executeBatch();
			
			for(String eventType : eventTypes) {
				/*batchStatement.addBatch(String.format("DROP TABLE IF EXISTS %s", eventType));
				batchStatement.addBatch(String.format("ALTER TABLE %s RENAME TO %s", (eventType + "_tmp"), eventType));
				System.out.println(String.format("ALTER TABLE %s RENAME TO %s", (eventType + "_tmp"), eventType));*/

				batchStatement.addBatch(String.format("DELETE FROM %s", eventType));
				batchStatement.addBatch(String.format("INSERT INTO %s SELECT * FROM %s", eventType,(eventType + "_tmp")));
			};
			batchStatement.executeBatch();

			batchStatement.executeUpdate("VACUUM");
			batchStatement.closeOnCompletion();
		} catch (SQLException e) {
			log.error("batch execute fuckup ... : {}", e.getMessage());
		}
		this.inBatchMode = false;
	}

	public void send(String type, Map<String, Object> data) {
		// fail fast
		if (!fields.containsKey(type)) {
			return;
		}
		
		try {
			
			if(inBatchMode) {

				if(!eventTypes.contains(type)) {
					// create original table
					batchStatement.addBatch(generateCreateTableClause(type, fields.get(type)));
					// create helpers
					// maybe delete from x here instead of drop?
					batchStatement.addBatch(String.format("DROP TABLE IF EXISTS %s", type + "_tmp"));
					batchStatement.addBatch(generateCreateTableClause(type + "_tmp", fields.get(type)));

					batchStatement.executeBatch();
					eventTypes.add(type);
				}
				batchStatement.addBatch(generateInsertClause(type + "_tmp", data, fields.get(type)));
			} else {
				/*Statement statement = connection.createStatement();
				if(!eventTypes.contains(type)) {
					if(dropOnInit) {
						statement.executeUpdate(String.format("DROP TABLE IF EXISTS %s", type));
					}
					try {
						statement.executeUpdate(generateCreateTableClause(type, fields.get(type)));
					} catch(org.sqlite.SQLiteException e) {
						if(e.getMessage().contains("already exists")) {
							eventTypes.add(type);
						} else {
							e.printStackTrace();
						}
					}
					eventTypes.add(type);
					doTruncate = false;
				}
				
				if(doTruncate) {
					
					for(String truncType : eventTypes) {
						statement.executeUpdate(String.format("DELETE FROM %s", eventTypes));
					}
					statement.executeUpdate("VACUUM");
					doTruncate = false;
				}
				statement.executeUpdate(generateInsertClause(type, data, fields.get(type)));
				statement.closeOnCompletion();*/
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** inputs **/
	
	private List<ReferenceInput> inputs = new ArrayList<ReferenceInput>();
	
	public List<ReferenceInput> getInputs() {
		return inputs;
	}

	public void addInput(ReferenceInput input) {
		inputs.add(input);
	}
}
