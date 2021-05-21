package com.github.arikastarvo.comet.input.file;

import java.io.File;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.InputConfiguration;
import com.github.arikastarvo.comet.input.InputDefinitionException;
import com.github.arikastarvo.comet.input.URICapableInputConfiguration;
import com.github.arikastarvo.comet.utils.FileSystem;

public class FileInputConfiguration extends InputConfiguration<FileInputConfiguration> implements URICapableInputConfiguration {

	public List<String> files = new ArrayList<String>();
	public int repeatInSeconds = 0;
	public StringReader reader; 
	public boolean tail = false;
	

	public FileInputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) { 
		super(monitorRuntimeConfiguration);
	}

	
	@Override
	public String getInputType() {
		return FileInput.NAME;
	}
	
	@Override
	public InputConfiguration parseMapInputDefinition(Map<String, Object> inputDefinition) throws InputDefinitionException {
		if (!inputDefinition.containsKey("file")) {
			throw new InputDefinitionException("file has to be specified for file input");
		}
		
		if(inputDefinition.get("file") instanceof String) {
			String pathValue = (String)inputDefinition.get("file");
			if(!pathValue.startsWith("/")) {
				pathValue = monitorRuntimeConfiguration.sourceConfigurationPath + File.separator + pathValue;
			}
			files.add(pathValue);
		} else if (inputDefinition.get("file") instanceof List) {
			files.addAll(((List<String>)inputDefinition.get("file")).stream().map( (String val) -> {
				return val.startsWith("/")?val:(monitorRuntimeConfiguration.sourceConfigurationPath + File.separator + val);
			}).collect(Collectors.toList()));
		} else {
			throw new InputDefinitionException("file has to be defined as a string or list of strings");
		}
		
		if(inputDefinition.containsKey("name") && inputDefinition.get("name") instanceof String) {
			this.name = (String)inputDefinition.get("name");
		}
		
		//this.files = new FileInputConfiguration(fileNames);
		if(inputDefinition.containsKey("tail") && inputDefinition.get("tail") instanceof Boolean) {
			this.tail = (Boolean)inputDefinition.get("tail");
		} if(inputDefinition.containsKey("repeat") && inputDefinition.get("repeat") instanceof Integer) {
			if(this.tail) {
				throw new InputDefinitionException("tail and repeat can not be defined together for file input");
			}
			this.repeatInSeconds = (Integer)inputDefinition.get("repeat");
		}
		return this;
	}
	
	@Override
	public Input<FileInput> createInputInstance() {

		FileInput input = new FileInput(this);
		if(this.name != null) {
			input.id = this.name;
		}
		return input;
	}
	

	/** uri parsing methods **/
	
	@Override
	public List<String> getSupportedSchemeList() {
		return Arrays.asList("file");
	}

	@Override
	public boolean isSchemeSupported(String scheme) {
		return getSupportedSchemeList().contains(scheme);
	}

	@Override
	public Map<String, Object> parseURIInputDefinition(URI inputDefinition) {

		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put("type", "file");
		
		String filename = FileSystem.getPathFromURI(inputDefinition);
		
		if(!filename.startsWith("/")) {
			filename = monitorRuntimeConfiguration.sourceConfigurationPath + File.separator + filename;
		}

		conf.put("file", Arrays.asList(filename));
		return conf;
	}
}
