package com.github.arikastarvo.comet.input.stdin;

import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.InputConfiguration;
import com.github.arikastarvo.comet.input.InputDefinitionException;
import com.github.arikastarvo.comet.input.URICapableInputConfiguration;

public class StdinInputConfiguration extends InputConfiguration<StdinInputConfiguration> implements URICapableInputConfiguration {

	
	public StdinInputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) { 
		super(monitorRuntimeConfiguration);
	}
	
	public InputStreamReader reader;

	@Override
	public StdinInputConfiguration parseMapInputDefinition(Map<String, Object> inputDefinition) throws InputDefinitionException {
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
		return "stdin";
	}

	@Override
	public StdinInput createInputInstance() {
		return new StdinInput(this);
	}

	/*@Override
	public <S extends Input<S>> Class<S> getInputClass() {
		return (Class<S>)StdinInput.class;
	}*/
	
	/** uri parsing interface methods **/

	@Override
	public List<String> getSupportedSchemeList() {
		return Arrays.asList("stdin");
	}
	
	@Override
	public Map<String, Object> parseURIInputDefinition(URI inputDefinition) {
		return new HashMap<String, Object>() {{
			put("type", "stdin");
		}};
	}
}
