package com.github.arikastarvo.comet;

import java.util.Map;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.internal.event.map.MapEventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.github.arikastarvo.comet.output.Output;

public class EventUpdateListener extends CustomUpdateListener {

	public EventUpdateListener(Output output) {
		super(output);
	}
	
	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
		//statement.getAnnotations();
		if ( newEvents != null ) { 
			for (EventBean event : newEvents) {
				
				// this thing replaces inline esper map objects with their underlying objects (maps)
				// it has an effect on new{foo='bar'} for example
				((Map<String, Object>)event.getUnderlying()).entrySet().forEach( (Map.Entry<String, Object> v) -> {
					if (v.getValue() instanceof MapEventBean) {
						v.setValue(((MapEventBean) v.getValue()).getUnderlying());
					}
				});
				output.printOutput(event.getUnderlying());
			}
		}
	}
}
