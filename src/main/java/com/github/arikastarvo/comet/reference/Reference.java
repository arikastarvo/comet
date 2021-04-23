package com.github.arikastarvo.comet.reference;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.github.arikastarvo.comet.InputEventReceiver;
import com.github.arikastarvo.comet.input.ReferenceInput;

public interface Reference extends InputEventReceiver {

	public String getName();
	public String getDriverClassName();
	public String getUrl();
	public Integer getLRUCacheSize();
	public Integer getCacheExpiryMaxAge();
	public Integer getCacheExpiryPurgeInterval();
	public Properties getProperties();
	public boolean dropOnInit();
	
	public List<ReferenceInput> getInputs();
	public void addInput(ReferenceInput input);

	public void setFields(Map<String, Map<String, String>> fields);

	public void setLRUCacheSize(int cacheSize);
	public void setCacheExpiryMaxAge(int cacheMaxAge);
	public void setCacheExpiryPurgeInterval(int cachePurgeInterval);
	public void setDropOnInit(boolean dropOnInit);
}
