package com.github.arikastarvo.comet.input;

import java.util.UUID;

import org.apache.logging.log4j.Logger;

import com.github.arikastarvo.comet.MonitorRuntime;
import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.runtime.MonitorRuntimeEsperImpl;

public abstract class Input<T extends Input<T>> extends Thread {

	/***
	 * This is used for pre-initsialization stuff (for adding event types, statements, etc)
	 * It will be injected after input object initialization, with init() mehtod
	 */
	public MonitorRuntimeConfiguration monitorRuntimeConfiguration;
	
	/***
	 * this is used for operating with events (sending them to runtime)
	 */
	public MonitorRuntime monitorRuntime;
	
	private boolean isShutdown = false;
	
	public Logger log;
	
	public String id;
	
	public InputConfiguration ic;
	
	public Input() {
		this.id = UUID.randomUUID().toString();
	}
	
	public <S extends InputConfiguration<S>> Input(InputConfiguration<S> inputConfiguration) {
		this.ic = inputConfiguration;
	}
	
	public T setId(String id) {
		this.id = id;
		return (T)this;
	}
	

	public void init(MonitorRuntimeConfiguration monitorRuntimeConfiguration) {
		this.monitorRuntimeConfiguration = monitorRuntimeConfiguration;
	}
	
	public void Stop() {
		monitorRuntime.stopInput(id);
		isShutdown = true;
	}
	
	public boolean isShutdown() {
		return isShutdown;
	}
	
	public void setAsShutdown() {
		isShutdown = true;
	}	

	public abstract String getDescription();

	public abstract void shutdown();
	
	public abstract InputConfiguration getInputConfiguration();
	
}
