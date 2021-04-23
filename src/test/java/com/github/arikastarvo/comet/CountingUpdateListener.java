package com.github.arikastarvo.comet;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.github.arikastarvo.comet.output.Output;

public class CountingUpdateListener extends EventUpdateListener {

	public int totalNewEvents = 0;
	
	public CountingUpdateListener(Output output) {
		super(output);
	}
	
	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
		totalNewEvents += newEvents.length;
	
		super.update(newEvents, oldEvents, statement, runtime);
	}

}
