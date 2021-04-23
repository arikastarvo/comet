package com.github.arikastarvo.comet.input.syslog;

import org.apache.logging.log4j.LogManager;

import com.github.arikastarvo.comet.input.Input;

/** untested and most likely not working **/
public class SyslogInput extends Input {

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
