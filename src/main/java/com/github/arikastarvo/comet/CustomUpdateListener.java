package com.github.arikastarvo.comet;

import com.espertech.esper.runtime.client.UpdateListener;
import com.github.arikastarvo.comet.output.Output;

public abstract class CustomUpdateListener implements UpdateListener {

	protected Output output;

	public CustomUpdateListener(Output output) {
		this.output = output;
	}
	
	public Output getOutput() {
		return output;
	}
	
	/*public void setOutput(Output output) {
		this.output = output;
	}*/
}
