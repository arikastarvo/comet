package com.github.arikastarvo.comet.persistence;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;

public class FilePersistence implements Persistence {

	String dataPath;
	Logger log = LoggerFactory.getLogger(FilePersistence.class);
	
	public FilePersistence() {
		this(null);
	}


	public FilePersistence(String dataPath) {
		log = LoggerFactory.getLogger(FilePersistence.class);
		if(dataPath != null && dataPath.length() > 0) {
			File pathObj = new File(dataPath);
			pathObj.mkdirs();
			if(!pathObj.exists() || !pathObj.isDirectory()) {
				log.error("could not create directory for data path '{}', using current dir as base", dataPath);
			} else {
				this.dataPath = pathObj.getAbsolutePath();
			}
		}
	}
	
	public String getPersistenceFilepath(String eventName) {
		String filePath;
		if(dataPath != null) {
			filePath = dataPath + File.separator + "persistence." + eventName + ".data";
		} else {
			filePath = "persistence." + eventName + ".data";
		}
		return filePath;
	}
	
	@Override
	public void persist(List<Map<String, Object>> data, String eventName) throws Exception {
	    BufferedWriter writer;
		String filePath = getPersistenceFilepath(eventName);
		
		try {
			writer = new BufferedWriter(new FileWriter(filePath));
		    writer.write(JsonStream.serialize(data));
		    writer.close();
		} catch (IOException e) {
			log.error("could not persist data to '{}' : {}", filePath, e.getMessage());
			log.debug("could not persist data to '{}' : {}", filePath, e.getMessage(), e);
			throw new Exception("could not persist", e);
		}
	}

	@Override
	public List<Map<String, Object>> load(String eventName) {
		log.debug("start loading persistence data for '{}'", eventName);
		InputStream inputStream = null;
		try {
			//String path = (dataPath != null)?(dataPath + File.separator):"";
			String filePath = getPersistenceFilepath(eventName);
		    File file = new File(filePath);
		    inputStream = new FileInputStream(file);
		    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
		    data = JsonIterator.deserialize(inputStream.readAllBytes()).as(data.getClass());
		    inputStream.close();
		    return data;
		} catch (Exception e) {
			log.warn("error during persistence data loading for '{}' (it just might be the first startup and no data exists yet): {}", eventName, e.getMessage());
			log.debug("error during persistence data loading for '{}' (it just might be the first startup and no data exists yet): {}", eventName, e.getMessage(), e);
		}
		return null;
	}


	@Override
	public void setMonitorRuntimeConfiguration(MonitorRuntimeConfiguration runtimeConfiguration) {
		// dummy method.. not needed
	}

}
