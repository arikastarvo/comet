package com.github.arikastarvo.comet.persistence;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class PersistenceConfiguration {

	public static final String __DEFAULT_STORAGE_TYPE__ = "file";
	
	public String storageType = __DEFAULT_STORAGE_TYPE__;
	public Map<String, Object> storageConfiguration;
	
	public List<String> persistence = new ArrayList<String>();
	public int persistenceInterval = 60;
	public boolean initialLoad = true;
}
