package com.github.arikastarvo.comet.persistence;

import java.util.List;
import java.util.Map;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;

public interface Persistence {
	
	public void persist(List<Map<String, Object>> data, String eventName) throws Exception;
	public List<Map<String, Object>> load(String eventName);
	
	public void setMonitorRuntimeConfiguration(MonitorRuntimeConfiguration runtimeConfiguration);
}
