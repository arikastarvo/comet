package com.github.arikastarvo.comet.output.file;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.output.Output;
import com.github.arikastarvo.comet.output.OutputConfiguration;
import com.github.arikastarvo.comet.output.OutputDefinitionException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

public class FileOutputConfiguration extends OutputConfiguration {

	public String template;
	public File outFile;
	
	public FileOutputConfiguration() {}
	
	public FileOutputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) { 
		super(monitorRuntimeConfiguration);
	}
	
	public FileOutputConfiguration(File file, String template) { this.outFile = file; this.template = template; }

	@Override
	public String getOutputType() {
		return FileOutput.NAME; 
	}
	
	@Override
	public FileOutputConfiguration parseMapOutputDefinition(Map outputDefinition) throws OutputDefinitionException {
		if(!outputDefinition.containsKey("file") && !outputDefinition.containsKey("name")) {
			throw new IllegalArgumentException("file output configuration must contain at least file or name parameters");
		}
		
		FileOutputConfiguration oc = new FileOutputConfiguration(monitorRuntimeConfiguration);
		String filename = outputDefinition.containsKey("file")?(String)outputDefinition.get("file"):((String)outputDefinition.get("name")) + ".log";
		
		if(!filename.startsWith("/") && monitorRuntimeConfiguration != null && monitorRuntimeConfiguration.applicationConfiguration != null && monitorRuntimeConfiguration.applicationConfiguration.logConfiguration.logPath != null) {
			filename = monitorRuntimeConfiguration.applicationConfiguration.logConfiguration.logPath + File.separator + filename;
		} else if(!filename.startsWith("/")) { // we have relative path, so prefix it with conf file path
			filename = monitorRuntimeConfiguration.sourceConfigurationPath + File.separator + filename;
		}
		oc.outFile = new File(filename);
		oc.template = (String)outputDefinition.get("out-format");
		
		return oc;
	}
	
	@Override
	public FileOutput createOutputInstance() {
		try {
			return new FileOutput(this);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
}
