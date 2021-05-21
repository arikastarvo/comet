package com.github.arikastarvo.comet.input.syslog;

import org.apache.logging.log4j.LogManager;

import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.InputConnector;

/** untested and most likely not working **/
@InputConnector(
	name = SyslogInput.NAME,
	configuration = SyslogInputConfiguration.class
)
public class SyslogInput extends Input {

	public static final String NAME = "syslog";

	SyslogInputConfiguration ic;
	
	public SyslogInput() {
		this.log = LogManager.getLogger(SyslogInput.class);
	}
	
	public SyslogInput(SyslogInputConfiguration ic) {
		this.log = LogManager.getLogger(SyslogInput.class);
		this.ic = ic;
	}
	
	@Override
	public SyslogInputConfiguration getInputConfiguration() {
		return this.ic;
	}

	@Override
	public void shutdown() {

		log.debug("stopping syslog input");
	}
	
	public String getDescription() {
		return "Syslog";
	}
}
