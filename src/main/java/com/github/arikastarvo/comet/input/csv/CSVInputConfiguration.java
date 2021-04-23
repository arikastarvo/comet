package com.github.arikastarvo.comet.input.csv;

import java.io.File;
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
import com.github.arikastarvo.comet.input.file.FileInputConfiguration;


public class CSVInputConfiguration extends InputConfiguration<CSVInputConfiguration> implements URICapableInputConfiguration {
	
	public String file;
	public boolean header = true;
	public Map<String, String> fields = new HashMap<String, String>();
	
	public boolean createAsWindow = true;
	
	public String content;


	public CSVInputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) { 
		super(monitorRuntimeConfiguration);
	}

	@Override
	public String getInputType() {
		return "csv";
	}

	@Override
	public CSVInputConfiguration parseMapInputDefinition(Map<String, Object> inputDefinition) throws InputDefinitionException {
		if(! inputDefinition.containsKey("file") && !inputDefinition.containsKey("content")) {
			throw new InputDefinitionException("for CSV input, file or content field must be set");
		} else if(! inputDefinition.containsKey("name") || ! (inputDefinition.get("name") instanceof String)) {
			throw new InputDefinitionException("for CSV input, name must be set");
		}  else {

			if(inputDefinition.containsKey("file")) {
				String pathValue = (String)inputDefinition.get("file");
				if(!pathValue.startsWith("/")) {
					pathValue = monitorRuntimeConfiguration.sourceConfigurationPath + File.separator + pathValue;
				}
				this.file = pathValue;
			}
			
			if(inputDefinition.containsKey("name")) {
				this.name = (String)inputDefinition.get("name");
			}
			
			if(inputDefinition.containsKey("content")) {
				this.content = (String)inputDefinition.get("content");
			}
			
			if(inputDefinition.containsKey("fields") && (inputDefinition.get("fields") instanceof Map)) {
				this.fields = (Map<String, String>)inputDefinition.get("fields");
			}
			
			if(inputDefinition.containsKey("window") && (inputDefinition.get("window") instanceof Boolean)) {
				this.createAsWindow = (Boolean)inputDefinition.get("window");
			}
			
			if(inputDefinition.containsKey("header") && (inputDefinition.get("header") instanceof Boolean)) {
				this.header = (Boolean)inputDefinition.get("header");
			}						
		}
		return this;
	}

	@Override
	public Input<CSVInput> createInputInstance() {
		CSVInput input = new CSVInput(this);
		input.id = this.name;
		return input;
	}
	
	/** uri parsing methods **/ 
	
	
	@Override
	public List<String> getSupportedSchemeList() {
		return Arrays.asList("csv");
	}

	@Override
	public boolean isSchemeSupported(String scheme) {
		return getSupportedSchemeList().contains(scheme);
	}

	@Override
	public Map<String, Object> parseURIInputDefinition(URI inputDefinition) {
		
		Map<String, Object> conf = new HashMap<String, Object>();
		
		// just reuse fileinput parsing function here
		FileInputConfiguration fic = new FileInputConfiguration(monitorRuntimeConfiguration);
		conf.putAll(fic.parseURIInputDefinition(inputDefinition));
		conf.put("type", "csv");
		conf.put("name", conf.get("file"));
		
		return conf;
	}
	
}
