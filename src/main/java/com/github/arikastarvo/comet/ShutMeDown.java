package com.github.arikastarvo.comet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ShutMeDown extends Thread {
	CometApplication app;
	Logger log = LogManager.getLogger(ShutMeDown.class);
	
	public ShutMeDown(CometApplication app) {
		log = LogManager.getLogger(ShutMeDown.class);
		this.app = app;
	}
	
	public void run() {
		shutdown();
	}
	
	public void shutdown() {
		log.info("shutting down gracefully...");
		
		// stop runtimes
		app.Stop();

		// shut down logging
		LogManager.shutdown();
	}
}
