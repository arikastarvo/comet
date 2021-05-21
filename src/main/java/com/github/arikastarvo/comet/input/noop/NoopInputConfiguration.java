package com.github.arikastarvo.comet.input.noop;

import java.util.Map;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.InputConfiguration;
import com.github.arikastarvo.comet.input.InputDefinitionException;

public class NoopInputConfiguration extends InputConfiguration<NoopInputConfiguration> {

	public boolean finite = false;
	
	public NoopInputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) { 
		super(monitorRuntimeConfiguration);
	}
	


	@Override
	public NoopInputConfiguration parseMapInputDefinition(Map<String, Object> inputDefinition) throws InputDefinitionException {
		if(!inputDefinition.containsKey("type")) {
			throw new InputDefinitionException("no type defined");
		}
		
		if(!inputDefinition.get("type").equals(getInputType())) {
			throw new InputDefinitionException(String.format("type has to be %s", getInputType()));
		}

		if(inputDefinition.containsKey("name")) {
			this.name = (String)inputDefinition.get("name");
		}
		
		if(inputDefinition.containsKey("finite") && inputDefinition.get("finite") instanceof Boolean) {
			this.finite = (Boolean)inputDefinition.get("finite");
		}
		
		return this;
	}

	@Override
	public String getInputType() {
		return NoopInput.NAME;
	}

	@Override
	public NoopInput createInputInstance() {

		NoopInput input = new NoopInput(this);
		input.id = this.name;
		return input;
	}
	
}
