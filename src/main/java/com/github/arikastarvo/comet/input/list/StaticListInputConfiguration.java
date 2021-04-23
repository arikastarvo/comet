package com.github.arikastarvo.comet.input.list;

import java.util.List;
import java.util.Map;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.InputConfiguration;
import com.github.arikastarvo.comet.input.InputDefinitionException;

public class StaticListInputConfiguration extends InputConfiguration<StaticListInputConfiguration> {

	public StaticListInputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) { 
		super(monitorRuntimeConfiguration);
	}
	
	public List<Object> content;
	public String field = "data";

	public boolean createAsWindow = true;


	@Override
	public StaticListInputConfiguration parseMapInputDefinition(Map<String, Object> inputDefinition) throws InputDefinitionException {
		if(!inputDefinition.containsKey("type")) {
			throw new InputDefinitionException("no type defined");
		}
		if(!inputDefinition.get("type").equals(getInputType())) {
			throw new InputDefinitionException(String.format("type has to be %s", getInputType()));
		}
		
		// legacy fallback when content field was data
		if(inputDefinition.containsKey("data") && ! inputDefinition.containsKey("content")) {
			inputDefinition.put("content", inputDefinition.get("data"));
		}
		
		if(! inputDefinition.containsKey("content") || ! (inputDefinition.get("content") instanceof List)) {
			throw new InputDefinitionException("for list input, data field must exist and be list");
		} else if(! inputDefinition.containsKey("name") || ! (inputDefinition.get("name") instanceof String)) {
			throw new InputDefinitionException("for list input, name must be set");
		}  else {
			this.name = (String)inputDefinition.get("name");
			this.content = (List<Object>)inputDefinition.get("content");
			if(inputDefinition.containsKey("field")) {
				this.field = (String)inputDefinition.get("field");
			}							
		}
		
		if(inputDefinition.containsKey("window") && (inputDefinition.get("window") instanceof Boolean)) {
			this.createAsWindow = (Boolean)inputDefinition.get("window");
		}
		return this;
	}

	@Override
	public String getInputType() {
		return "list";
	}

	@Override
	public StaticListInput createInputInstance() {

		StaticListInput input = new StaticListInput(this);
		input.id = this.name;
		return input;
	}
	
}
