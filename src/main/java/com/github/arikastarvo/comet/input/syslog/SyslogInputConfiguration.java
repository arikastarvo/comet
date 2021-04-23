package com.github.arikastarvo.comet.input.syslog;

import java.util.Map;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.InputConfiguration;
import com.github.arikastarvo.comet.input.InputDefinitionException;

public class SyslogInputConfiguration extends InputConfiguration<SyslogInputConfiguration> {
	
	public SyslogInputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) { 
		super(monitorRuntimeConfiguration);
	}


	@Override
	public SyslogInputConfiguration parseMapInputDefinition(Map<String, Object> inputDefinition) throws InputDefinitionException {
		if(!inputDefinition.containsKey("type")) {
			throw new InputDefinitionException("no type defined");
		}
		
		if(!inputDefinition.get("type").equals(getInputType())) {
			throw new InputDefinitionException(String.format("type has to be %s", getInputType()));
		}

		if(inputDefinition.containsKey("name")) {
			this.name = (String)inputDefinition.get("name");
		}
		
		return this;
	}

	@Override
	public String getInputType() {
		return "noop";
	}

	@Override
	public SyslogInput createInputInstance() {

		SyslogInput input = new SyslogInput(this);
		input.id = this.name;
		return input;
	}
	
}
