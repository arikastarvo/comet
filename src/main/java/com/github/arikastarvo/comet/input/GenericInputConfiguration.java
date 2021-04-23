package com.github.arikastarvo.comet.input;

import java.util.Map;

public class GenericInputConfiguration extends InputConfiguration<GenericInputConfiguration> {

	private String type;
	
	@Override
	public String getInputType() {
		return type;
	}

	@Override
	public InputConfiguration<GenericInputConfiguration> parseMapInputDefinition(Map<String, Object> inputDefinition) {
		
		if(inputDefinition.containsKey("type")) {
			this.type = (String)inputDefinition.get("type"); 
		}
		if(inputDefinition.containsKey("name")) {
			this.name = (String)inputDefinition.get("name");
		}
		return this;
	}

	@Override
	public Input createInputInstance() {
		// TODO Auto-generated method stub
		return null;
	}

}
