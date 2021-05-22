package com.github.arikastarvo.comet.output.stdout;

import java.io.File;
import java.util.Map;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.output.OutputConfiguration;
import com.github.arikastarvo.comet.output.OutputDefinitionException;

public class StdoutOutputConfiguration extends OutputConfiguration {
	public String template;
	

	public StdoutOutputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) { 
		super(monitorRuntimeConfiguration);
	}
	
	public StdoutOutputConfiguration(String template) { this.template = template; }
	
	@Override
	public String getOutputType() {
		return StdoutOutput.NAME;
	}
	@Override
	public StdoutOutputConfiguration parseMapOutputDefinition(Map outputDefinition) throws OutputDefinitionException {

		
		String currentOutFormat = null;
		if(outputDefinition.containsKey("out-format")) {
			currentOutFormat = (String)outputDefinition.get("out-format");
			File tmpFile = new File(monitorRuntimeConfiguration.sourceConfigurationPath + File.separator + currentOutFormat); 
			if(tmpFile.exists()) {
				currentOutFormat = tmpFile.getAbsolutePath();
			}
		}
		this.template = currentOutFormat;
		return this;
	}
	@Override
	public StdoutOutput createOutputInstance() {
		return new StdoutOutput(this);
	}
}
