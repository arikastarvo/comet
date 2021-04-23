package com.github.arikastarvo.comet.input;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.csv.CSVInputConfiguration;
import com.github.arikastarvo.comet.input.file.FileInputConfiguration;
import com.github.arikastarvo.comet.input.list.StaticListInputConfiguration;
import com.github.arikastarvo.comet.input.noop.NoopInputConfiguration;
import com.github.arikastarvo.comet.input.stdin.StdinInputConfiguration;

public abstract class InputConfiguration<T extends InputConfiguration<T>> extends HashMap<String, Object> {

	
	protected Logger log;
	
	protected MonitorRuntimeConfiguration monitorRuntimeConfiguration;
	
	public String name;
	
	protected InputConfiguration() { }
	private InputConfiguration(Object[] args) { }
	
	public InputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) {
		super();
		this.monitorRuntimeConfiguration = monitorRuntimeConfiguration;
	}
	
	public static List<Class> getInputConfigurationClasses() {
		return Arrays.asList(
				CSVInputConfiguration.class,
				FileInputConfiguration.class,
				StaticListInputConfiguration.class,
				StdinInputConfiguration.class,
				NoopInputConfiguration.class
		);
	}

	public abstract String getInputType();
	public abstract InputConfiguration<T> parseMapInputDefinition(Map<String, Object> inputDefinition) throws InputDefinitionException;
	//public abstract <S extends Input<S>> Class<S> getInputClass();
	public abstract Input createInputInstance();
	
}

