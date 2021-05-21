package com.github.arikastarvo.comet.input.list;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;

import com.github.arikastarvo.comet.InputEventReceiver;
import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.FiniteInput;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.InputConnector;
import com.github.arikastarvo.comet.input.ReferenceInput;
import com.github.arikastarvo.comet.reference.ReferenceReloadCallback;

@InputConnector(
	name = StaticListInput.NAME,
	configuration = StaticListInputConfiguration.class
)
public class StaticListInput extends Input<StaticListInput> implements FiniteInput, ReferenceInput {

	public static final String NAME = "list";

	private String type = "string";
	
	private StaticListInputConfiguration ic = null;
	
	private InputEventReceiver ier = null;
	private ReferenceReloadCallback callback;
	
	public StaticListInput(StaticListInputConfiguration ic) {
		this.log = LogManager.getLogger(StaticListInput.class);
		this.ic = ic;
	}
	
	@Override
	public StaticListInputConfiguration getInputConfiguration() {
		return this.ic;
	}
	
	@Override
	public void shutdown() {
		log.debug("stopping static list input '{}'", id);
	}

	@Override
	public String getDescription() {
		return String.format("static list '%s'", id);
	}
	
	public void init(MonitorRuntimeConfiguration monitorRuntimeConfiguration) {		
		super.init(monitorRuntimeConfiguration);

		if(this.ic.createAsWindow) {
			
			String createFields = String.format("%s %s", ic.field, type);
			
			String publicEventType = String.format("@Name(\"%s\") @Tag(name=\"silent\", value=\"true\") @public @buseventtype create schema %s (%s %s);", id + "Event", id + "Event", ic.field, type);
			String createQuery = String.format("@Name(\"%s\") @Tag(name=\"silent\", value=\"true\") @Public create window %s#keepall (%s);", id, id, createFields);
			String insertFields = ic.field;
			String insertQuery = String.format("@Name(\"InsertInto%s\") @Tag(name=\"silent\", value=\"true\") insert into %s select %s from %s;", id, id, insertFields, id + "Event");

			/*log.debug(publicEventType);
			log.debug(createQuery);
			log.debug(insertQuery);*/
			monitorRuntimeConfiguration.addExtraStatement(publicEventType + " " + createQuery + " " + insertQuery);
			
		} else {
			// just create new event type
			monitorRuntimeConfiguration.addEventType(id, new HashMap<String, Object>() {{
				put(ic.field, type);
			}});
		}
		
	}

	public void run() {

		if(callback != null) {
			callback.reloadInit();
		}
		
		if(ier == null) {
			ier = this.monitorRuntime;
		}
		ic.content.forEach( it -> { 
			Map<String, Object> obj = new HashMap<String, Object>();
			obj.put(ic.field, it);
			String eventType = id + (this.ic.createAsWindow?"Event":"");
			obj.put("eventType", eventType);
			ier.send(eventType, obj);
		});

		if(callback != null) {
			callback.reloadFinalize();
		}
		Stop();
	}
	
	@Override
	public boolean isFinite() {
		return true;
	}

	@Override
	public void setInputEventReceiver(InputEventReceiver ier) {
		this.ier = ier;
	}

	@Override
	public void setReloadCallback(ReferenceReloadCallback callback) {
		this.callback = callback;
	}

}
