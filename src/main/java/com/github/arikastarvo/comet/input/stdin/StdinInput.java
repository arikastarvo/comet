package com.github.arikastarvo.comet.input.stdin;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;

import com.github.arikastarvo.comet.CometApplication;
import com.github.arikastarvo.comet.input.Input;

public class StdinInput extends Input<StdinInput> {

	StdinInputConfiguration ic;

	/*public StdinInput() {
		this(null);
	}*/
	
	public StdinInput(StdinInputConfiguration ic) {
		
		this.log = LogManager.getLogger(StdinInput.class);
		
		this.ic = ic;
		
		if(ic.name != null) {
			this.id = ic.name;
		}
	}
	
	@Override
	public StdinInputConfiguration getInputConfiguration() {
		return this.ic;
	}

	public void run() {
		this.log = LogManager.getLogger(StdinInput.class);
		this.ic.reader = new InputStreamReader(System.in);
		try {
			//new ReaderInput(id, app, this.ic);
			reader();
		} catch (IOException e) {
			log.error("error during stdin input init: " + e.getMessage());
			log.debug("error during stdin input init: " + e.getMessage(), e);
		}		
	}

	private void reader() throws IOException {

		BufferedReader br = new BufferedReader(ic.reader);
		String line;
		while ((line = br.readLine()) != null) {
			monitorRuntime.parseAndSend(line, null, this.id);
		}
	}

	@Override
	public void shutdown() {
		log.debug("stopping stdin input");
	}
	
	public String getDescription() {
		return "Stdin";
	}
}
