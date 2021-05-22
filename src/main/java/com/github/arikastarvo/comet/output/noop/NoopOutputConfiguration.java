package com.github.arikastarvo.comet.output.noop;

import java.util.Map;

import com.github.arikastarvo.comet.output.Output;
import com.github.arikastarvo.comet.output.OutputConfiguration;
import com.github.arikastarvo.comet.output.OutputDefinitionException;

public class NoopOutputConfiguration extends OutputConfiguration {

	@Override
	public String getOutputType() {
		return NoopOutput.NAME;
	}

	@Override
	public NoopOutputConfiguration parseMapOutputDefinition(Map outputDefinition) throws OutputDefinitionException {
		return this;
	}

	@Override
	public Output createOutputInstance() {
		return new NoopOutput(this);
	}

}
