package com.github.arikastarvo.comet;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CometApplicationConfiguration {

	Logger log = LogManager.getLogger(CometApplicationConfiguration.class);

	
	/**
	 * 
	 * GENERAL
	 * 
	 */
	
	public String applicationName = "comet-" + new SimpleDateFormat("YYYY-MM-dd_HH-mm-ss").format(new Date());
	
	/**
	 * This defaults to system(user.dir)
	 * If --basepath is set, it will overwrite this param.
	 * 
	 */
	public String basepath = System.getProperty("user.dir");
	
	public String runningHostname; 
	public String runningDC;
	
	/** if instance is daemonized or not **/
	public boolean daemonize = false; 
	
	public String defaultDataPath = basepath;
	
	public List<String> profiles = new ArrayList<String>();
	
	public class LogConfiguration {
		
		public String logPath;
		
		public Level defaultLevel = Level.INFO;
		
		public boolean hasBeenConfigured = false;
		
		public boolean coloredFatal = false;
		
		public boolean socketEnabled = false;
		
		public boolean infoEnabled = false;
		public String infoFile = "comet.log";
		
		public boolean debugEnabled = false;
		public String debugFile = "comet-debug.log";
		public List<String> debugComponents = new ArrayList<String>() {{ add(CometApplication.LOG_COMPONENT_COMET); }};
		
		public boolean debugLogsSeparate = true;
		
		public boolean isEnabled() {
			return infoEnabled || debugEnabled;
		}
		
		public boolean isInfoStdout() {
			return infoFile.equals("-") || infoFile.equals("stdout");
		}
		
		public boolean isDebugStdout() {
			return debugFile.equals("-") || debugFile.equals("stdout");
		}
		
		public String getFullInfoPath() {
			if(logPath != null) {
				return logPath + File.separator + infoFile;
			} else {
				return infoFile;
			}
		}
		
		public String getFullDebugPath() {
			if(logPath != null) {
				return logPath + File.separator + debugFile;
			} else {
				return debugFile;
			}
		}
		
		public String getFullDebugPath(String component) {
			String fileName = component.equals(CometApplication.LOG_COMPONENT_COMET)?debugFile:component + "-" + debugFile;
			if(logPath != null) {
				return logPath + File.separator + fileName;
			} else {
				return fileName;
			}
		}
		
		public String toString() {
			return
				"info: " + (infoEnabled?"true":"false") + " > " + infoFile + "\n" +
				"debug: " + (debugEnabled?"true":"false") + " > " + debugFile;
		}
	}
	
	public LogConfiguration logConfiguration = new LogConfiguration();
	
	public static String getRunningHostname() {

		String runningHostname = System.getenv("HOSTNAME");

		Logger log = LogManager.getLogger(CometApplicationConfiguration.class);
		if(runningHostname == null) {
			try {
				runningHostname = InetAddress.getLocalHost().getCanonicalHostName().toLowerCase();
				if (runningHostname != null) {
					log.debug("setting current hostname as " + runningHostname + " from InetAddress");
				}
			} catch (UnknownHostException e) {
				log.debug("could not get hostname via InetAddress");
				log.debug("setting current hostname as " + runningHostname + " from HOSTNAME env");
			}
		} else {
			runningHostname = runningHostname.toLowerCase();
		}
		return runningHostname;
	}
	
	public CometApplicationConfiguration() {

		log = LogManager.getLogger(CometApplicationConfiguration.class);
		
		runningHostname = getRunningHostname();

		if(runningHostname != null) {
			if(runningHostname != null && runningHostname.indexOf("-") > 0) {
				runningDC = runningHostname.substring(0, runningHostname.indexOf("-"));
			} else if (runningHostname != null) {
				runningDC = runningHostname;
			}
			
		}
	}
	
}
