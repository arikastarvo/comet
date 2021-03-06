package com.github.arikastarvo.comet.runtime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.annotation.Description;
import com.espertech.esper.common.client.annotation.Tag;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.configuration.common.ConfigurationCommonDBRef;
import com.espertech.esper.common.client.configuration.common.ConfigurationCommonEventTypeMap;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeDestroyedException;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.EPUndeployException;
import com.github.arikastarvo.comet.CometApplication;
import com.github.arikastarvo.comet.CustomUpdateListener;
import com.github.arikastarvo.comet.EventUpdateListener;
import com.github.arikastarvo.comet.InputEventReceiver;
import com.github.arikastarvo.comet.MonitorRuntime;
import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.FiniteInput;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.file.FileInput;
import com.github.arikastarvo.comet.output.file.FileOutput;
import com.github.arikastarvo.comet.output.file.FileOutputConfiguration;
import com.github.arikastarvo.comet.output.stdout.StdoutOutput;
import com.github.arikastarvo.comet.output.stdout.StdoutOutputConfiguration;
import com.github.arikastarvo.comet.parser.Parser;
import com.github.arikastarvo.comet.persistence.PersistenceManager;
import com.github.arikastarvo.comet.utils.DNSUtil;
import com.github.arikastarvo.comet.utils.MaxMind;
import com.github.arikastarvo.comet.utils.NetUtil;
import com.jsoniter.output.JsonStream;

public class MonitorRuntimeEsperImpl extends MonitorRuntime implements InputEventReceiver {
	
	public MonitorRuntimeEsperImpl(MonitorRuntimeConfiguration configuration) {
		super(configuration);
	}

	public EPRuntime runtime = null;
	
	//public PersistenceManager persistenceManager;
	

	Logger log = LogManager.getLogger(MonitorRuntimeEsperImpl.class);
	
    public void run() {
		ThreadContext.put("monitor", configuration.runtimeName);
		log.debug("initializing monitor '{}'", configuration.runtimeName);
		
		// initialize persistence manager
		try {
			this.persistenceManager = new PersistenceManager(this);
		} catch(Exception e) {
			log.error("failed to create persistence manager, continuing without one. error: {}", e.getMessage());
			log.debug("failed to create persistence manager, continuing without one. error: {}", e.getMessage(), e);
		}

		// initialize outputs
		configuration.getListeners().values().forEach( listener -> listener.getOutput().init(configuration));
		// initialize inputs (here we load last preconfigured event types)
		configuration.getInputs().values().forEach( input -> input.init(configuration));
		
		// initialize default parser if there aren't any parsers yet (and there exists input that does not have explicit parser) or if there isn't any inputs defined (default input needs parser)
		Boolean allInputsHaveParser = configuration.getInputsIdsWithPatterns().containsAll(configuration.getInputs().keySet());
		if((!allInputsHaveParser || configuration.getInputs().size() == 0) && getParsers().size() == 0) {
			try {
				log.debug("creating default parser");
				addParser(DEFAULT_PARSER_ID, new Parser(configuration.getDefaultPatternReferences(), configuration.getDefaultPatterns(), configuration.getEventTypes(), false, configuration.usePatternset, configuration.getDefaultEventTypesToParse()));
			} catch(Exception e) {
				log.error("could not create default parser");
			}
		}
		
		// now initialize input specific parsers
		// TODO! actually every input should have it's own parser (this way custom patterns and/or event-types to be parsed can have local effect)
		if(configuration.getInputsIdsWithPatterns().size() > 0 ) {
			for(String inputId : configuration.getInputsIdsWithPatterns()) {
				try {
					log.debug("creating dedicated parser for input {}", inputId);
					// now this is a half-solution for now because half of the configuration params used are general
					addParser(inputId, new Parser(configuration.getPatternReferences(inputId), configuration.getPatterns(inputId), configuration.getEventTypes(), false, false, configuration.getDefaultEventTypesToParse()));
					
					// and now we use input-id as parser id.. so what was the point of mapping then..?
					inputToParserMapping.put(inputId, inputId);
				} catch(Exception e) {
					log.error("could not create parser for input '{}'", inputId);
				}
			}
		}
		/*if(configuration.getInputPatterns().size() > 0) {
			for(Map.Entry<String, List<String>> entry : configuration.getInputPatterns().entrySet()) {
				try {
					// now this is a half-solution for now because half of the configuration params used are general
					addParser(entry.getKey(), new Parser(entry.getValue(), configuration.getEventTypes(), false, configuration.usePatternset));
					// and now we use input-id as parser id.. so what was the point of mapping then..?
					inputToParserMapping.put(entry.getKey(), entry.getKey());
				} catch(Exception e) {
					log.error("could not create parser for input '{}'", entry.getKey());
				}
			}
		}*/
		 
		
		if(!configuration.hasStatements()) {
			log.debug("no statement given - setting default esper statement");
			configuration.addStatement(CometApplication.DEFAULT_EPL_STATEMENT);
		}
		
		/** COMMAND LINE ARG OUTPUTS **/

		// set stdout output if no other output is defined
		if(configuration.getListeners().size() == 0 && !configuration.applicationConfiguration.daemonize) {
			log.debug("no outputs configured, setting stdout");
			this.addStdoutOutput();
		}
		
		// start stdin input
		if(!configuration.hasInputs() && !configuration.applicationConfiguration.daemonize) {
			log.debug("no inputs configured, adding stdin input as default");
			this.addStdinInput();
		}

		// if only file inputs defined, then set extrnal clocking by default
		if(configuration.getInputs().values().stream().filter((Input in) -> !(in instanceof FileInput)).count() == 0 && configuration.getInputs().values().stream().filter((Input in) -> (in instanceof FileInput) && ((FileInput)in).tail()).count() == 0 ) {
			log.debug("All configured inputs are non-tailing fileInputs, setting external clocking to true");
			configuration.externalClock = true;
		}
		
		try {
			this.registerEsperStatements();
		} catch(RuntimeException e) {
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).error("Unrecoverable situation, exiting.");
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).debug("Unrecoverable situation, exiting.", e);
		} catch (EPCompileException e) {
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).error("Syntax error in EPL query: " + e.getMessage());
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).debug("Syntax error in EPL query: " + e.getMessage(), e);
		} catch (EPDeployException e) {
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).error("EPL query deployment error: " + e.getMessage());
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).debug("EPL query deployment error: " + e.getMessage(), e);
		} catch (com.espertech.esper.common.client.module.ParseException e) {
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).error("EPL module parse failed: " + e.getMessage());
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).debug("EPL module parse failed: " + e.getMessage(), e);
		} catch (Exception e) {
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).error("Unknown error during EPL query compilation and deployment: " + e.getMessage());
			LogManager.getLogger(MonitorRuntimeEsperImpl.class).debug("Unknown error during EPL query compilation and deployment: " + e.getMessage(), e);
		}
		

		// pre-load persisted data to esper
		if(this.persistenceManager != null) {
			this.persistenceManager.loadAndSchedulePeristence();
		}

		// do dummy load on secrets to log startup secrets
		Map<String, Map<String, Object>> secrets = this.getApplicationSecrets();
		if(secrets != null && secrets.size() > 0) {
			log.debug("Usable secrets during bootstrap : {}", secrets.keySet());
		} else {
			log.debug("No usable secrets found during bootstrap");
		}
		secrets = null;
		
		// let the data flow
		this.hookInputs(waitInput);
		
		this.running = true;
		
		this.startedAt = System.currentTimeMillis();
	}
	
	public String addParser(Parser parser) {
		String id = UUID.randomUUID().toString();
		return addParser(id, parser);
	}
	
	public String addParser(String id, Parser parser) {
		this.parsers.put(id, parser);
		return id;
	}
	
	public Parser getParser() {
		return parsers.get(DEFAULT_PARSER_ID);
	}
	
	public Map<String, Parser> getParsers() {
		return parsers;
	}
	
	public Parser getParser(String inputId) {
		return (inputId != null && inputToParserMapping.containsKey(inputId) && parsers.containsKey(inputToParserMapping.get(inputId)))?parsers.get(inputToParserMapping.get(inputId)):parsers.get(DEFAULT_PARSER_ID);
	}

	public void removeAllStatements() {
		configuration.removeAllStatements();
	}

	public String addStatement(String statement) {
		return configuration.addStatement(UUID.randomUUID().toString(), statement, null);
	}
	
	public String addStatement(String uuid, String statement) {
		return configuration.addStatement(uuid, statement, null);
	}
	
	/*public String addAndRegisterStatement(String statement) {
		String uuid = configuration.addStatement(uuid, statement, null);
		
		return uuid;
	}*/
	
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
	public void hookInputs() {
		hookInputs(false);
	}
	
	/**
	 * lets insert some data to application
	 * 
	 *  @param wait - wait until batch inputs have completed before returning (for tests)
	 * 
	 */
	public void hookInputs(boolean wait) {
		
		try {
			int startedInputs = 0;
			List<String> stopInputs = new ArrayList<String>();
			for(Input in : configuration.getInputs().values()) {
	
				in.monitorRuntime = this;
				log.debug("starting input '{}' with id '{}'", in.getClass().getName(), in.id);
				if(wait && FiniteInput.class.isAssignableFrom(in.getClass()) && ((FiniteInput)in).isFinite()) {
					in.run();
					stopInputs.add(in.id);
				} else {
					in.start();
				}
				
				startedInputs += 1;
			}
			
			stopInputs.forEach( (String inputId) -> {
				stopInput(inputId);
			});
			
			log.debug("started {} inputs during startup ({} finished)", startedInputs, stopInputs.size());
			
		} catch (Exception e) {
			log.error("Something unexpected happened during input startup: {}", e.getMessage());
			log.debug("Something unexpected happened during input startup: {}", e.getMessage(), e);
		}
	}
	
	/*** input management ***/

	/*public void addStdinInput() {
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
	
	public void addUnixSocketOutput() throws IOException {
		UnixSocketOutput out = new UnixSocketOutput(new UnixSocketOutputConfiguration());
		configuration.addListener(new EventUpdateListener(out));
	}
	
	public UnixSocketOutput getUnixSocketOutput() throws IOException {
		Optional<CustomUpdateListener> cul = configuration.getListeners().values().stream().filter(listener -> listener.getOutput() instanceof UnixSocketOutput).findFirst();
		if(cul.isPresent()) {
			return (UnixSocketOutput)cul.get().getOutput();
		} else {
			return null;
		}
	}
	
	public Map<String, Input> getInputs() {
		return configuration.getInputs();
	}*/
	
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
	
	public void addListenerToAllStatements(String id, EventUpdateListener listener) {
		this.deployedStatements.forEach( (key, statementDefinition) -> {
			EPStatement stat = (EPStatement) statementDefinition.get("statement");
			log.debug("adding listener '{}' to statement '{}' (from statement group '{}')", id, statementDefinition.get("id"),  statementDefinition.get("internalDeploymentId"));
			stat.addListener(listener);
		});
		this.recalculateDeploymentsDependsOn();
	}
	
	/** logging configuration ***/
	
	static LoggerContext initializeLogging() {

		ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();

		// create root logger and set it to off
		RootLoggerComponentBuilder rootLogger   = builder.newRootLogger(Level.OFF);
		builder.add(rootLogger);
		return Configurator.initialize(builder.build());
	}
	
	public void registerEsperStatements(String statement) throws IOException, RuntimeException, EPCompileException, EPDeployException, com.espertech.esper.common.client.module.ParseException {
		registerEsperStatements(Arrays.asList(statement));
	}
	
	public void registerEsperStatements(List<String> statement) throws IOException, RuntimeException, EPCompileException, EPDeployException, com.espertech.esper.common.client.module.ParseException {
		registerEsperStatements(statement, false);
	}
	
	public void registerEsperStatements(List<String> statementList, boolean externalClock) throws IOException, RuntimeException, EPCompileException, EPDeployException, com.espertech.esper.common.client.module.ParseException {
		configuration.removeAllStatements();
		for (String statement : statementList) {
			configuration.addStatement(statement);
		}
		registerEsperStatements();
	}
	
	public void registerEsperStatements() throws IOException, RuntimeException, EPCompileException, EPDeployException, com.espertech.esper.common.client.module.ParseException {
		// run the shit
		log.info("Starting Esper runtime with " + (configuration.externalClock?"external":"internal") + " timing");
		
		/** we have statement, so let's setup esper **/
		if(configuration.hasStatements()) {
			Configuration conf = new Configuration();
			
			/** register lookup references **/
			if (configuration.lookupReferences.size() > 0) {
				configuration.lookupReferences.forEach( (refName, ref) -> {
					log.debug("registering reference '{}' with url '{}'", refName, ref.getUrl());
					ConfigurationCommonDBRef dbref = new ConfigurationCommonDBRef();
					if(ref.getLRUCacheSize() != null) {
						dbref.setLRUCache(ref.getLRUCacheSize());	
					}
					if(ref.getCacheExpiryMaxAge() != null && ref.getCacheExpiryPurgeInterval() != null) {
						dbref.setExpiryTimeCache(ref.getCacheExpiryMaxAge(), ref.getCacheExpiryPurgeInterval());
					}

					Properties props = ref.getProperties();
					props.put("monitor", refName);
					
					dbref.setDataSourceFactory(props, BasicDataSourceFactory.class.getName());
					//dbref.setDataSourceFactory(props, SQLiteReferenceDataSourceFactory.class.getName());
					//dbref.setDriverManagerConnection(className, url, connectionArgs);
					
					// for in-memory sqlites, we have to retain one connection
					dbref.setConnectionLifecycleEnum(ConfigurationCommonDBRef.ConnectionLifecycleEnum.RETAIN);
					
					//dbref.setConnectionReadOnly(true);
					//dbref.setDriverManagerConnection(ref.getDriverClassName(), ref.getUrl(), ref.getProperties());
					conf.getCommon().addDatabaseReference(refName, dbref);
					
					
					// now start ref inputs
					// i'm quite sure that this isn't actually the correct way to do things.. these inputs should be "hooked" with all others
					// but we can't bootstrap these during regular hooking because the order of loading (these inputs should be started before esper)
					ref.getInputs().forEach( input -> {
						Input genericInputObj = (Input)input;
						genericInputObj.monitorRuntime = this;
						log.debug("starting reference input '{}' with id '{}'", genericInputObj.getClass().getName(), genericInputObj.id);
						
						genericInputObj.init(configuration);
						if(FiniteInput.class.isAssignableFrom(genericInputObj.getClass()) && ((FiniteInput)genericInputObj).isFinite()) {
							genericInputObj.run();
						} else {
							genericInputObj.start();
						}
					});
				});
				
				/*Properties props = new Properties();
				
				props.put("driverClassName", "org.sqlite.JDBC");
				props.put("url", "jdbc:sqlite:/tmp/sample.db");
				
				//props.put("initialSize", 2);
				props.put("validationQuery", "select 1");
				
				ConfigurationCommonDBRef dbref = new ConfigurationCommonDBRef();
				dbref.setLRUCache(10);
				dbref.setExpiryTimeCache(1, 1);
				//dbref.setConnectionLifecycleEnum(ConfigurationCommonDBRef.ConnectionLifecycleEnum.POOLED);
				//dbref.setDataSourceFactory(props, BasicDataSourceFactory.class.getName());
				dbref.setDriverManagerConnection("org.sqlite.JDBC", "jdbc:sqlite:/tmp/sample.db", props);
				conf.getCommon().addDatabaseReference("sitt", dbref);*/
			}
			
			/** start metrics **/			
			
			//conf.getCommon().addEventType(RuntimeMetric.class);
			conf.getRuntime().getMetricsReporting().setRuntimeInterval(1000);
			conf.getRuntime().getMetricsReporting().setStatementInterval(1000);
			conf.getRuntime().getMetricsReporting().setThreading(false);
			
			//conf.getRuntime().getMetricsReporting().setEnableMetricsReporting(true);
			//conf.getRuntime().getMetricsReporting().setJmxRuntimeMetrics(true);
			/** END metrics **/
			
			/** UDF functions **/
			conf.getCompiler().addPlugInSingleRowFunction("ipin", NetUtil.class.getCanonicalName(), "ipin");
			conf.getCompiler().addPlugInSingleRowFunction("isprivateip", NetUtil.class.getCanonicalName(), "isprivateip");
			conf.getCompiler().addPlugInSingleRowFunction("notprivateip", NetUtil.class.getCanonicalName(), "notprivateip");
			
			conf.getCompiler().addPlugInSingleRowFunction("asn", MaxMind.class.getCanonicalName(), "asn");
			conf.getCompiler().addPlugInSingleRowFunction("asname", MaxMind.class.getCanonicalName(), "asnName");
			conf.getCompiler().addPlugInSingleRowFunction("cc", MaxMind.class.getCanonicalName(), "cc");
			conf.getCompiler().addPlugInSingleRowFunction("ccname", MaxMind.class.getCanonicalName(), "ccName");
			
			conf.getCompiler().addPlugInSingleRowFunction("dns_lookup", DNSUtil.class.getCanonicalName(), "dns_lookup");
			
			// legacy
			conf.getCompiler().addPlugInSingleRowFunction("isremoteip", NetUtil.class.getCanonicalName(), "notprivateip");
			
			
			// debugging
			conf.getCompiler().addPlugInSingleRowFunction("netrange", NetUtil.class.getCanonicalName(), "netrange");

			/** END UDF functions **/

			/*conf.getCommon().getEventMeta().getAvroSettings().setEnableAvro(true);
			conf.getCommon().getEventMeta().getAvroSettings().setEnableNativeString(true);
			conf.getCommon().getEventMeta().getAvroSettings().setEnableSchemaDefaultNonNull(true);
			conf.getCommon().getEventMeta().getAvroSettings().setObjectValueTypeWidenerFactoryClass(null);
			conf.getCommon().getEventMeta().getAvroSettings().setTypeRepresentationMapperClass(null);*/
			
			
			// register default parser event types
			List<String> registeredTypes = new ArrayList<String>();
			getParsers().forEach( (String id, Parser parser) -> {
				parser.getPatterns().forEach((reg) -> {
					Map<String, Object> fields = (Map<String, Object>) reg.get("fields");
					fields.put("eventType", "string");
					fields.put("logts_timestamp", "long");
					
					ConfigurationCommonEventTypeMap typeConf = new ConfigurationCommonEventTypeMap();
					typeConf.setStartTimestampPropertyName("logts_timestamp");
					typeConf.setEndTimestampPropertyName("logts_timestamp");
					if(reg.hasParents()) {
						typeConf.setSuperTypes(new HashSet<String>(reg.getParents()));
					}
					log.trace("registering event type '{}' with fields: {}", (String)reg.get("name"), fields.keySet());
					registeredTypes.add((String)reg.get("name"));
					conf.getCommon().addEventType(reg.getName(), fields, typeConf);
				});
			});
			log.debug("registered event types: {}", registeredTypes);

			//runtime = EPRuntimeProvider.getDefaultRuntime(this.configuration.runtimeName, conf);
			runtime = EPRuntimeProvider.getRuntime(this.configuration.runtimeName, conf);
			
			if(configuration.externalClock) {
				runtime.getEventService().clockExternal();
				if(configuration.initialTime >= 0) {
					log.debug("setting initial time to " + configuration.initialTime);
					runtime.getEventService().advanceTime(configuration.initialTime);
				}
			}
			
			// deploy extra statements
			for(String statement : configuration.getExtraStatements()) {
				deployModule(UUID.randomUUID().toString(), new HashMap<String, String>() {{
					put("statement", statement);
					put("type", "extra");
				}}, conf);
			}
			
			// deploy regular statements
			for( Map.Entry<String, Map<String, String>> entry : configuration.getStatements().entrySet() ) {

				String id = entry.getKey();
				Map<String, String> statement = entry.getValue();
				deployModule(id, statement, conf);
			};
			
			recalculateDeploymentsDependsOn();
		}
	}
	
	public String getDeploymentIdFromInternalDeploymentId(String internalDeploymentId) throws NoSuchElementException {
		String deploymentId;
		
		try {
			deploymentId = (String) deployments.values().stream().filter( (Map dep) -> dep.containsKey("internalDeploymentId") && dep.get("internalDeploymentId").equals(internalDeploymentId)).findFirst().get().get("id");
		} catch (NoSuchElementException e) {
			throw new NoSuchElementException("No such module " + internalDeploymentId);
		}
		return deploymentId;
	}
	
	public String getUndeployedDeploymentIdFromInternalDeploymentId(String internalDeploymentId) throws NoSuchElementException {
		String deploymentId;
		try {
			deploymentId = (String) undeployedDeployments.values().stream().filter( (Map dep) -> dep.containsKey("internalDeploymentId") && dep.get("internalDeploymentId").equals(internalDeploymentId)).findFirst().get().get("id");
		} catch (NoSuchElementException e) {
			throw new NoSuchElementException("No such module " + internalDeploymentId);
		}
		return deploymentId;
	}
	
	public void undeployModule(String moduleId) throws EPRuntimeDestroyedException, EPUndeployException, NoSuchElementException {
		
		Map<String, Object> deployment;
		String deploymentId;
		try {
			deploymentId = getDeploymentIdFromInternalDeploymentId(moduleId);
			deployment = deployments.get(deploymentId);
			if(deployment == null) {
				throw new NoSuchElementException("No such module found: " + moduleId);
			}
		} catch (NoSuchElementException e) {
			throw new NoSuchElementException("No such deployment found: " + moduleId);
		}
		
		if(deployments.containsKey(deployment.get("id"))) {
			runtime.getDeploymentService().undeploy(deploymentId);
			undeployedDeployments.put(deploymentId, deployments.remove(deploymentId));
			List<String> statIds = deployedStatements.values().stream().filter( (Map stat) -> stat.get("deploymentId").equals(deploymentId)).map( (Map stat) -> (String)stat.get("name")).collect(Collectors.toList());
			for(String statId : statIds) {
				undeployedStatements.put(statId, deployedStatements.remove(statId));
			}
			
			log.debug("module '{}' undeployed", moduleId);
		} else {
			throw new NoSuchElementException("No such module " + moduleId);
		}

		recalculateDeploymentsDependsOn();
	}
	
	public void redeployModule(String moduleId) throws EPRuntimeDestroyedException, EPCompileException, EPDeployException, IOException, com.espertech.esper.common.client.module.ParseException {

		Map<String, Object> oldDeployment;
		Map<String, String> moduleDefinition = new HashMap<String, String>();
		String oldDeploymentId;
		try {
			oldDeploymentId = getUndeployedDeploymentIdFromInternalDeploymentId(moduleId);
			oldDeployment = undeployedDeployments.get(oldDeploymentId);
			if(oldDeployment == null) {
				throw new NoSuchElementException("No such module " + moduleId);
			}
			moduleDefinition.put("statement", (String)oldDeployment.get("statement"));
			moduleDefinition.put("type", (String)oldDeployment.get("type"));
			moduleDefinition.put("filename", (String)oldDeployment.get("filename"));
			
		} catch (NoSuchElementException e) {
			throw new NoSuchElementException("No such module " + moduleId);
		}

		
		if(undeployedDeployments.containsKey(oldDeployment.get("id"))) {
			deployModule(moduleId, moduleDefinition, runtime.getConfigurationDeepCopy());
			undeployedDeployments.remove(oldDeploymentId);

			log.debug("module '{}' redeployed", moduleId);
		} else {
			throw new NoSuchElementException("No such module " + moduleId);
		}
	}
	
	public void deployModule(File file) throws Exception { 
		if(file.exists() && file.isFile()) {
			
			try {
				byte[] encoded = Files.readAllBytes(Paths.get(file.getAbsolutePath()));
				
				//statement.add(new String(encoded, "UTF-8"));
				Map<String, String> statement = new HashMap<String, String>() {{
					put("statement", new String(encoded, "UTF-8"));
					put("type", "file");
					put("filename", file.getAbsolutePath());
				}};
				deployModule(file.getName(), statement, this.runtime.getConfigurationDeepCopy());
				
			} catch (IOException e) {
				// this is normal, skip ahead
			}
		} else {
			throw new Exception("No such file");
		}
	}
	
	public void deployModule(String statement) throws EPRuntimeDestroyedException, EPCompileException, EPDeployException, IOException, com.espertech.esper.common.client.module.ParseException {
		
		Map<String, String> statementMap = new HashMap<String, String>() {{
			put("statement", statement);
			put("type", "other");
		}};
		
		Integer max = 0;
		try {
			max = deployments.keySet().stream().filter( (String id) -> id.matches("^module-[0-9]+$")).map( (String id) -> Integer.parseInt(id.substring(14))).max(Integer::compare).get();
		} catch(NoSuchElementException e) {
			// pass
		}
		deployModule("module-" + String.valueOf(++max), statementMap, this.runtime.getConfigurationDeepCopy());
	}
	
	private void deployModule(String internalDeploymentId, Map<String, String> statement, Configuration conf) throws EPCompileException, EPRuntimeDestroyedException, EPDeployException, IOException, com.espertech.esper.common.client.module.ParseException {

		log.debug("deploying module with internalDeploymentId: {}", internalDeploymentId);
		
		/*String id = entry.getKey();
		Map<String, String> statement = entry.getValue();*/
		
		// Build compiler arguments
		CompilerArguments args = new CompilerArguments(conf);
		
		// Make the existing EPL objects available to the compiler
		args.getPath().add(runtime.getRuntimePath());
		
		String statementEPL = statement.get("statement");
		
		
		com.espertech.esper.common.client.module.Module module = EPCompilerProvider.getCompiler().parseModule(statementEPL);
		if (statement.containsKey("type") && statement.get("type").equals("file") && statement.containsKey("filename")) {
			module.setName(statement.get("filename"));
		}
		EPCompiled compiled = EPCompilerProvider.getCompiler().compile(module, args);
		

		// Compile		
		//EPCompiled compiled = EPCompilerProvider.getCompiler().compile(statementEPL, args);
		
		// Return the deployment
		EPDeployment epDeployment = runtime.getDeploymentService().deploy(compiled);
		
		
		// fill in deployemnts information for later use
		if (!deployments.containsKey(epDeployment.getDeploymentId())) {
			deployments.put(epDeployment.getDeploymentId(), new HashMap<String, Object>() {{
				put("dependencies", Arrays.asList(epDeployment.getDeploymentIdDependencies()));
				put("statement", statement.get("statement"));
				put("type", statement.get("type"));
				put("id", epDeployment.getDeploymentId());
				put("internalDeploymentId", internalDeploymentId);
				if(statement.containsKey("filename")) {
					put("filename", statement.get("filename"));
				}
			}});
			log.debug("deployed module with internalId {} as deploymentId: {}", internalDeploymentId, epDeployment.getDeploymentId());
		} else {
			log.warn("Deployment with id '{}' already exists, can't deploy that one. wierd uh?", epDeployment.getDeploymentId());
		}
		log.debug("output2statement: {}", configuration.outputToStatementRedirection);
		Arrays.asList(epDeployment.getStatements()).forEach( (EPStatement stat) -> {
			
			log.debug("deploying statement name:{}, id: {}", stat.getName(), stat.getDeploymentId());
			
			AtomicReference<Boolean> silent = new AtomicReference<Boolean>(false);
			
			Arrays.asList(stat.getAnnotations()).forEach( annotation -> {
				if(annotation instanceof Tag) {
					if(((Tag)annotation).name().toString().equals("silent") && ((Tag)annotation).value().toString().equals("true")) {
						silent.set(true);
					}
				}
			});
		
			String desc = null;
			try {
				desc = ((Description)Arrays.asList(stat.getAnnotations()).stream().filter(it -> it instanceof Description).findFirst().get()).value();
			} catch (NoSuchElementException e) {
				// pass
			}

			Map<String, Object> depStatement = new HashMap<String, Object>();
			if(desc != null) {
				depStatement.put("desc", desc);
			}
			depStatement.put("eventType", stat.getEventType());
			depStatement.put("deploymentId", stat.getDeploymentId());
			depStatement.put("internalDeploymentId", internalDeploymentId);
			depStatement.put("listeners", new ArrayList<String>());
			depStatement.put("statement", stat);
			
			String statementID = stat.getName();
			if(deployedStatements.containsKey(stat.getName())) {
				statementID = UUID.randomUUID().toString();
				log.warn("duplicate statement name, assiging random id '{}' for statement '{}'", statementID, stat.getName());
			}
			depStatement.put("name", stat.getName());
			depStatement.put("id", statementID);
			
			deployedStatements.put(statementID, depStatement);

			if(stat.getName().startsWith("__") || silent.get()) {
				// we have silet flag set in the EPL query, skip output for this one
				return;
			}
			log.debug("this is dep statement {}", depStatement.get("id"));
			AtomicReference<Boolean> hasOutput = new AtomicReference<Boolean>(false);
			configuration.getListeners().forEach( (key, listener) -> {

				// check if the output has any specific query binded to it
				if(configuration.outputToStatementRedirection.containsKey(key)) {
					// we check if this statement (or whole deployment) is registered to given output
					if(!configuration.outputToStatementRedirection.get(key).contains(depStatement.get("name")) &&
							!configuration.outputToStatementRedirection.get(key).contains(depStatement.get("internalDeploymentId"))) {
						log.debug("skipping output {} for dep {}", key, depStatement.get("name"));
						return;
					}
				}
				// check if this output has been binded to any statements
				/*if(configuration.statementDeplymentToOutputRedirection.entrySet().stream().filter((Map.Entry<String, List<String>> entry) -> entry.getValue().contains(key)).count() > 0) {
					// of binded, but not to this statement, then we skip it
					if(configuration.statementDeplymentToOutputRedirection.entrySet().stream().filter((Map.Entry<String, List<String>> entry) -> entry.getKey().equals(internalDeploymentId) && entry.getValue().contains(key)).count() == 0) {
						return;
					}
				}*/
				hasOutput.set(true);
				log.debug("adding listener '{}' to statement '{}' (from statement group '{}')", key, depStatement.get("id"), internalDeploymentId);
				stat.addListener(listener);
				((List<String>)deployedStatements.get(depStatement.get("id")).get("listeners")).add(listener.getOutput().getClass().getSimpleName());
			});
			if(!hasOutput.get()) {
				log.warn("statement '{}' from module '{}' was not marked as silent but no outputs was configured for it also", depStatement.get("id"), module.getName());
			}
			
		});
	}
	
	private void recalculateDeploymentsDependsOn() {
		for(Map.Entry<String, Map<String, Object>> deployment : deployments.entrySet()) {
			List<String> providesDependecies = new ArrayList<String>();
			runtime.getDeploymentService().getDeploymentDependenciesProvided(deployment.getKey()).getDependencies().forEach( dep -> {
				dep.getDeploymentIds().forEach( depId -> {
					if(!providesDependecies.contains(depId)) {
						providesDependecies.add(depId);
					}
				});
			});
			deployments.get(deployment.getKey()).put("provides", providesDependecies);
		}
	}
	
	public void destroy() {
		if(runtime != null) {
			runtime.destroy();
		}
	}
	
	public void Stop() {

		// stop all inputs ( so that no more new data would come)
		stopInputs();

		// persist existing states if configured so
		if(persistenceManager != null) {
			persistenceManager.stopScheduledPersist();
			persistenceManager.persistencePersist();
		}

		// destroy esper runtime
		destroy();

		// finally, stop all outputs
		stopOutputs();

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
	
	public void intoRuntime(String type, Map<String, Object> result) {
		
		if(type == null || type.trim().equals("")) {
			type = CometApplication.DEFAULT_EVENT_TYPE;
		}
		if(configuration.externalClock) {
			if(result.containsKey("logts_timestamp") && (long) result.get("logts_timestamp") > 0) {
				long newTime = ((long) result.get("logts_timestamp"));
				if(newTime > runtime.getEventService().getCurrentTime()) {
					//runtime.getEventService().advanceTime(newTime);
					runtime.getEventService().advanceTimeSpan(newTime, 1000);
				}
	
			}/* this effed up most of the local tests .. this should be actually necessary i think
			else if(runtime.getEventService().isExternalClockingEnabled() && runtime.getEventService().getCurrentTime() < System.currentTimeMillis()) {
				runtime.getEventService().advanceTime(System.currentTimeMillis());
			}*/
		}
		//this.counter.incrementAndGet();
		runtime.getEventService().sendEventMap(result, type);
	}
	
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
	public List<Map<String, Object>> fireAndForget(String statement, boolean outputStdout) throws EPCompileException, IOException, com.espertech.esper.common.client.module.ParseException, EPRuntimeDestroyedException, EPDeployException, EPUndeployException {
		// get esper configuration
		Configuration conf = this.runtime.getConfigurationDeepCopy();
		// create compiler args
		CompilerArguments args = new CompilerArguments(conf);
		// Make the existing EPL objects available to the compiler
		args.getPath().add(runtime.getRuntimePath());
		// parse module
		//com.espertech.esper.common.client.module.Module module = EPCompilerProvider.getCompiler().parseModule(statement);
		// compile statement
		EPCompiled compiled = EPCompilerProvider.getCompiler().compileQuery(statement, args);
		List<Map<String, Object>> objects = new ArrayList<Map<String, Object>>();
		try {
			// execute
			runtime.getFireAndForgetService().executeQuery(compiled).iterator().forEachRemaining( (EventBean bean) -> {
				if(outputStdout) {
					System.out.println(JsonStream.serialize(bean.getUnderlying()));
				}
				Map<String, Object> mapObj = new HashMap<String, Object>();
				for(String prop : bean.getEventType().getPropertyNames()) {
					Object value = null;
					Class type = bean.getEventType().getPropertyType(prop);
					if(type.equals(long.class)) {
						value = bean.get(prop);

					} else if (type.equals(int.class) && bean.get(prop) instanceof String){
						if(((String)bean.get(prop)).length() > 0) {
							value = Integer.parseInt((String)bean.get(prop));
						}
					} else {
						value = (bean.getEventType().getPropertyType(prop).cast(bean.get(prop)));
					}
					mapObj.put(prop, value);
				}
				objects.add(mapObj);
			});
		} catch (Exception e) {
			log.error("Fire and forget failed : {}", e.getMessage());
			/* if we are dealing with event stream (not data table/window), then register as regular statement ???
			
			EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
			
			// dwstroy when ??
			runtime.getDeploymentService().undeploy(deployment.getDeploymentId());*/
		}
		return objects;
	}

	@Override
	public long getNumEventsEvaluated() {
		return runtime.getEventService().getNumEventsEvaluated();
	}
	
}
