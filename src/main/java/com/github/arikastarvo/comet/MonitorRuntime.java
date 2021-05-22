package com.github.arikastarvo.comet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPRuntimeDestroyedException;
import com.espertech.esper.runtime.client.EPUndeployException;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.file.FileInput;
import com.github.arikastarvo.comet.input.file.FileInputConfiguration;
import com.github.arikastarvo.comet.input.noop.NoopInput;
import com.github.arikastarvo.comet.input.noop.NoopInputConfiguration;
import com.github.arikastarvo.comet.input.stdin.StdinInput;
import com.github.arikastarvo.comet.input.stdin.StdinInputConfiguration;
import com.github.arikastarvo.comet.output.file.FileOutput;
import com.github.arikastarvo.comet.output.file.FileOutputConfiguration;
import com.github.arikastarvo.comet.output.stdout.StdoutOutput;
import com.github.arikastarvo.comet.output.stdout.StdoutOutputConfiguration;
import com.github.arikastarvo.comet.parser.Parser;
import com.github.arikastarvo.comet.persistence.PersistenceManager;

public abstract class MonitorRuntime extends Thread implements InputEventReceiver {
	
	// matchers
	public final static String DEFAULT_PARSER_ID = "DEFAULT_PARSER";
	public Map<String, Parser> parsers = new HashMap<String, Parser>();
	public Map<String, String> inputToParserMapping = new HashMap<String, String>();

	public Map<String, Map<String, Object>> undeployedDeployments = new HashMap<String, Map<String,Object>>();
	public Map<String, Map<String, Object>> deployments = new HashMap<String, Map<String,Object>>();
	public Map<String, Map<String, Object>> undeployedStatements = new HashMap<String, Map<String,Object>>();
	public Map<String, Map<String, Object>> deployedStatements = new HashMap<String, Map<String,Object>>();
	
	public boolean running = false;
	public boolean dirty = false;
	
	public long startedAt = 0L;
	
	public MonitorRuntimeConfiguration configuration;
	
	public PersistenceManager persistenceManager;

	Logger log = LogManager.getLogger(MonitorRuntime.class);

	public Boolean waitInput = false;
	
	public MonitorRuntime(MonitorRuntimeConfiguration configuration) {
		this.configuration = configuration;
		// set thread name
		this.setName(this.configuration.runtimeName);
	}
	
	/**
	 * run the damn thing
	 * 
	 */
    public abstract void run();
	
    // parser manipulation
    
	public abstract String addParser(Parser parser);
	
	public abstract String addParser(String id, Parser parser);
	
	public abstract Parser getParser();
	
	public abstract Parser getParser(String inputId);
	
	public abstract Map<String, Parser> getParsers();

	// statement manipulation
	
	public abstract void removeAllStatements();

	public abstract String addStatement(String statement);
	
	public abstract String addStatement(String uuid, String statement);
	
	// app property management
	
	public Properties getApplicationProperties() {
		Properties appProps = new Properties();
		try {
			InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties");
			if(is != null) {
				appProps.load(is);
			}
		} catch (FileNotFoundException e) {
			log.debug("no application properties file, must be in dev mode");
		} catch (IOException e) {
			log.error("could not load application properties file : {}", e.getMessage());
			log.debug("could not load application properties file : {}", e.getMessage(), e);
		}
		return appProps;
	}

	/*** secrets management ***/
	
	public Map<String, Map<String, Object>> getApplicationSecrets() {
		return configuration.getApplicationSecrets();
	}
	
	public Map<String, Object> getApplicationSecret(String secret) {
		return configuration.getApplicationSecret(secret);
	}
	
	/** end secrets-management **/
	
	public String getApplicationVersion() {
		Properties props = getApplicationProperties();
		return props.getProperty("version", "DEV");
	}
	
	public void stopInput(String inputId) {
		if(configuration.getInputs().containsKey(inputId)) {
			configuration.getInputs().get(inputId).shutdown();
			configuration.removeInput(inputId);
			log.debug("stopped and removed input {}; {} more are left", inputId, configuration.getInputs().size());
			if(configuration.getInputs().size() == 0) {
				Stop();
			}
		}
	}
	
	public void stopInputs() {
		log.debug("Stopping inputs");
		for(Input in : configuration.getInputs().values()) {
			if(!in.isShutdown()) {
				in.shutdown();
			}
		}
	}

	/**
	 * lets insert some data to application
	 */
	public abstract void hookInputs();
	
	/**
	 * lets insert some data to application
	 * 
	 *  @param wait - wait until batch inputs have completed before returning (for tests)
	 * 
	 */
	public abstract void hookInputs(boolean wait);
	
	/*** input management ***/
	
	/**
	 * Add dummy input without any functionality. Rarely useful
	 */
	public void addDummyInput() {
		NoopInput di = new NoopInput(new NoopInputConfiguration(configuration));
		configuration.addInput(di);
	}

	public void addStdinInput() {
		StdinInput si = new StdinInput(new StdinInputConfiguration(configuration));
		si.setId("stdin");
		configuration.addInput(si);
	}
	
	public void addFileInput(File file) {
		FileInputConfiguration ic = new FileInputConfiguration(configuration);
		ic.files = Arrays.asList(file.getAbsolutePath());
		FileInput fi = new FileInput(ic);
		fi.setId("file-" + file.getName());
		fi.start();
		configuration.addInput(fi);
	}
	
	public void addTailFileInput(File file) {
		FileInputConfiguration ic = new FileInputConfiguration(configuration);
		ic.files = Arrays.asList(file.getAbsolutePath());
		ic.tail = true;
		FileInput fi = new FileInput(ic);
		fi.setId("tail-" + file.getName());
		fi.start();
		configuration.addInput(fi);
	}
	
	public Map<String, Input> getInputs() {
		return configuration.getInputs();
	}
	
	//////
	// output management 
	//////
	
	
	public void stopOutputs() {
		log.debug("Stopping outputs");
		for(CustomUpdateListener listener : configuration.getListeners().values()) {
			listener.getOutput().Stop();
		}
	}
	
	public void addFileOutput(File file) throws FileNotFoundException {
		FileOutput fo = new FileOutput(new FileOutputConfiguration(file, null));
		
		String listenerId = "file-" + file.getName();
		EventUpdateListener listener = new EventUpdateListener(fo);
		// different redirection mechanisms here?
		configuration.addListener(listenerId, listener);
		
		this.addListenerToAllStatements(listenerId, listener);
	}
	
	public void addStdoutOutput() {
		configuration.addListener("stdout", new EventUpdateListener(new StdoutOutput(new StdoutOutputConfiguration(configuration.outputFormat))));
	}
	
	public Map<String, CustomUpdateListener> getListeners() {
		return configuration.getListeners();
	}
	
	public abstract void addListenerToAllStatements(String id, EventUpdateListener listener);
	
	
	/**
	 * initialize logging
	 * 
	 * @return
	 */
	static LoggerContext initializeLogging() {

		ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();

		// create root logger and set it to off
		RootLoggerComponentBuilder rootLogger   = builder.newRootLogger(Level.OFF);
		builder.add(rootLogger);
		return Configurator.initialize(builder.build());
	}
	
	// module managemnet

	public abstract void deployModule(File file) throws Exception;
	
	public abstract void redeployModule(String moduleId) throws Exception;

	public abstract void undeployModule(String moduleId) throws Exception;
	
	public abstract void deployModule(String statement) throws Exception;
	
	
	public abstract void destroy();
	
	public void Stop() {
		destroy();
		stopInputs();
		stopOutputs();
		
		if(persistenceManager != null) {
			persistenceManager.stopScheduledPersist();
		}
		Thread.currentThread().interrupt();
		this.running = false;
		this.dirty = true;
		try {
			CometApplication.getApplication().unloadMonitorRuntime(configuration.runtimeName);
		} catch (Exception e) {
			log.error("could not stop/unload monitor: {}", e.getMessage());
			log.debug("could not stop/unload monitor: {}", e.getMessage(), e);
		}
	}
	
	public void parse(String line, Map<String, Object> result) {
		handleLine(line, result, false, configuration.keepMatches, null);
	}
	
	public void parse(String line, Map<String, Object> result, String inputId) {
		handleLine(line, result, false, configuration.keepMatches, inputId);
	}
	
	public void parse(String line, Map<String, Object> result, Boolean keepMatches, String inputId) {
		handleLine(line, result, false, keepMatches, inputId);
	}
	
	public void parseAndSend(String line, Map<String, Object> result) {
		handleLine(line, result, true, configuration.keepMatches, null);
	}
	
	public void parseAndSend(String line, Map<String, Object> result, String inputId) {
		handleLine(line, result, true, configuration.keepMatches, inputId);
	}
	
	public void send(String type, Map<String, Object> data) {
		intoRuntime(type, data);
	}
	
	private void handleLine(String line, Map<String, Object> result, boolean addToEsperRuntime, boolean keepMatchType, String inputId) {
		if(result == null) {
			result = new HashMap<String, Object>();
		}
		
		getParser(inputId).matchline(line, result);
		
		List matchedTypes;
		if(keepMatchType) {
			matchedTypes = (List)result.get("__match");
		} else {
			matchedTypes = (List)result.remove("__match");
		}
		
		if(configuration.removeRawData && result.containsKey("data")) {
			result.remove("data");
		}
		
		if(result.containsKey("logts")) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssX");
		    try {
				Date parsedDate = dateFormat.parse((String)result.get("logts"));
				result.put("logts_timestamp", parsedDate.getTime());
			} catch (ParseException e) {
				// pass
			}
		}
		if(addToEsperRuntime) {
			intoRuntime((String)matchedTypes.get(matchedTypes.size()-1), result);
		}
	}
	
	public abstract void intoRuntime(String type, Map<String, Object> result);
	
	public abstract long getNumEventsEvaluated();
	
	/**
	 * 
	 * TODO
	 * 
	 * ALPHA status feature !!!
	 * 
	 * This is a work-in-progress part of feature that should allow to execute fire-and-forget queries over jmx 
	 * 
	 * @param statement
	 * @throws EPCompileException
	 * @throws IOException
	 * @throws com.espertech.esper.common.client.module.ParseException
	 * @throws EPRuntimeDestroyedException
	 * @throws EPDeployException
	 * @throws EPUndeployException
	 */
	public abstract List<Map<String, Object>> fireAndForget(String statement, boolean outputStdout) throws Exception;
	
}
