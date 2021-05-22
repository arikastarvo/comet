package com.github.arikastarvo.comet.output.noop;

import java.util.Map;

import com.github.arikastarvo.comet.output.Output;
import com.github.arikastarvo.comet.output.OutputConfiguration;
import com.github.arikastarvo.comet.output.OutputConnector;

@OutputConnector(
	name = NoopOutput.NAME,
	configuration = NoopOutputConfiguration.class
)
public class NoopOutput extends Output {

	public static final String NAME = "noop";
	
	NoopOutputConfiguration oc = null;
	
	public NoopOutput() {
		oc = new NoopOutputConfiguration();
	}
	public NoopOutput(NoopOutputConfiguration oc) {
		this.oc = oc;
	}
	
	public void printOutput(Map<String, Object> data) {
		//
	}
	
	public void printOutput(Object data) {
		//
	}
	
	public void printOutput(String data) {
		//
	}

	public void Stop() {
		//
	}
	
	public String getDescription() {
		return "Noop";
	}

	@Override
	public <T extends OutputConfiguration<T>> OutputConfiguration<T> getOutputConfiguration() {
		return this.oc;
	}
}