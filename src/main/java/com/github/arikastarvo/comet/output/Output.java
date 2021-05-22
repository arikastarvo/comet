package com.github.arikastarvo.comet.output;

import java.util.Map;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;

public abstract class Output {

	/***
	 * This is used for pre-initsialization stuff
	 * It will be injected after output object initialization, with init() mehtod
	 */
	protected MonitorRuntimeConfiguration monitorRuntimeConfiguration;

	
	public OutputConfiguration oc;
	
	public abstract void printOutput(Map<String, Object> data);
	public abstract void printOutput(Object data);	
	public abstract void printOutput(String data);
	
	public abstract void Stop();
	
	public abstract String getDescription();
	
	public abstract <T extends OutputConfiguration<T>> OutputConfiguration<T> getOutputConfiguration();
	
	public void init(MonitorRuntimeConfiguration monitorRuntimeConfiguration) {
		this.monitorRuntimeConfiguration = monitorRuntimeConfiguration;
	}
}