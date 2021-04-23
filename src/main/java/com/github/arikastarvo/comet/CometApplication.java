package com.github.arikastarvo.comet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.LoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.yaml.snakeyaml.Yaml;

import com.github.arikastarvo.comet.runtime.MonitorRuntimeEsperImpl;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class CometApplication {
	
	
	private static CometApplication app = null;
	
	/** this is temp placeholder for bootstrap (app is not fully initialized) **/
	private static String appName = null;
	
	private long startedAt = 0L;
	
	public final static String DEFAULT_EVENT_TYPE = "events";
	public final static String DEFAULT_EPL_STATEMENT = "select * from " + DEFAULT_EVENT_TYPE;
	
	public String DEFAULT_MONITOR_ID = "default";
	
	public final static String LOG_COMPONENT_COMET = "comet";
	public final static String LOG_COMPONENT_ESPER = "esper";
	
	public final static List<String> LOG_COMPONENTS = Arrays.asList(LOG_COMPONENT_COMET, LOG_COMPONENT_ESPER);

	
	public Map<String, MonitorRuntime> monitors = new HashMap<String, MonitorRuntime>(); 
	public Map<String, File> monitorsConfigurationFiles = new HashMap<String, File>(); 
	
	public CometApplicationConfiguration configuration = new CometApplicationConfiguration();
	
	public Boolean validationMode = false;
	
	Logger log;
	
	private static String help =
		"Query logs using Esper EPL language\n\n" +
		"             stdin,files   regex,json   Esper EPL   stdout,file\n" +
		"                  |              |          |            |\n" +
		"Program flow: data input -> parse data -> query -> result output";
	
	private static String extendedHelp =
		"External configuration file usage\n" +
		"========================\n" +
		"Configuration file is in yaml format.\n" +
		"Command line arguments override configuration file settings.\n" +
		"See README.md for more help.";
	
	public static CometApplication getApplication() {
		return app;
	}
	
	private static void setApplication(CometApplication app) {
		CometApplication.app = app;
	}
	
	public static String getApplicationName() {
		if(app != null && app.configuration != null) {
			return app.configuration.applicationName;
		} else {
			return CometApplication.appName;
		}
	}
	
	public static long getApplicationPID() {
		return ProcessHandle.current().pid();
	}
	
	public static String getApplicationRunningUser() {
		return System.getProperty("user.name");
	}
	
	public long getApplicationStartedAt() {
		return startedAt;
	}
	
	 
	public static void main(String[] args) throws Exception {
		
		app = new CometApplication(args);
		app.init(args);
		setApplication(app);
		
		
		Runtime.getRuntime().addShutdownHook(new ShutMeDown(app));
		
		if(app.validationMode) {
			System.exit(0);
		}
		
		app.startedAt = System.currentTimeMillis();
		
		LogManager.getLogger(CometApplication.class).debug("Startup complete");
	}
	
	public CometApplication(String[] args) throws Exception {
		setApplication(this);
	}
	
	public void init(String[] args) throws Exception {
		
		ArgumentParser parser = ArgumentParsers.newFor("comet").addHelp(false).build()
			.description(CometApplication.help);

		ArgumentGroup generalArgGroup = parser.addArgumentGroup("General comet configuration params");
		generalArgGroup.addArgument("-n", "--name")
			.type(String.class)
			.metavar("name")
			.help("Set instance name (must be unique per running host)");
		
		generalArgGroup.addArgument("-c")
			.type(String.class)
			.action(Arguments.append())
			.metavar("conf")
			.help("Start program with pre-set configuration");
		
		generalArgGroup.addArgument("--daemonize")
			.action(Arguments.storeTrue())
			.type(Boolean.class)
			.setDefault(false)
			.help("adds a dummy input instead of stdin as a default. this way daemonized instance keeps running as there is an active input.");
		
		generalArgGroup.addArgument("--interactive")
			.action(Arguments.storeTrue())
			.type(Boolean.class)
			.setDefault(false)
			.help("Indicates that output is attached to a tty, so by default colored error level logging is enabled");
				
		generalArgGroup.addArgument("--data-path")
			.type(String.class)
			.help("Data path for temp files or persistence storage (default will be current dir)");
		
		generalArgGroup.addArgument("--basepath")
			.type(String.class)
			.help("basepath to use istead of system(user.dir)");
		
		generalArgGroup.addArgument("--secrets")
			.type(String.class)
			.setDefault("conf/secrets.yaml")
			.help("set path to secrets configuration (yaml conf file). used for setting some general external connection secrets via configuration management tools (from vaults).");

		generalArgGroup.addArgument("--profile")
			.type(String.class)
			.action(Arguments.append())
			.metavar("profile")
			.help("set profiles");

		
		generalArgGroup.addArgument("-h", "--help")
			.action(Arguments.storeTrue())
			.help("show short help message and exit");
		
		generalArgGroup.addArgument("-H", "--extended-help")
			.action(Arguments.storeTrue())
			.help("show extended help message and exit");
		
		generalArgGroup.addArgument("-v", "--version")
			.action(Arguments.storeTrue())
			.setDefault(false)
			.help("print version and exit");
		
		generalArgGroup.addArgument("--validate")
			.action(Arguments.storeTrue())
			.setDefault(false)
			.help("validate epl syntax and exit (return code 0 for all ok, >0 otherwise");
		
		generalArgGroup.addArgument("--xmx")
			.help("just a placeholder, doesn't do anything");
		
		/** LOGGING **/
		ArgumentGroup logArgGroup = parser.addArgumentGroup("Logging configuration (disabled by default)");
		logArgGroup.addArgument("--log")
			.type(String.class)
			.setDefault("")
			.help("enable logging (- means stdout, everything else is path to file)");
		
		logArgGroup.addArgument("--log-path")
			.type(String.class)
			.help("set additional log path (useful for when configuring logfile names via config file and giving path via cmd line arg)");
		
		logArgGroup.addArgument("--debug")
			.action(Arguments.storeTrue())
			.setDefault(false)
			.help("enable debug logging (if file logging is configured, then \"debug-\" is prepended to info level filename");
		
		logArgGroup.addArgument("--debug-esper")
			.action(Arguments.storeTrue())
			.setDefault(false)
			.help("enable debug logging for esper packages");
		
		logArgGroup.addArgument("--debug-enable-all")
			.action(Arguments.storeTrue())
			.setDefault(false)
			.help("Enable debug logging for all components. By default only comet internal debug logging is enabled. Components - " + LOG_COMPONENTS);
		
		logArgGroup.addArgument("--debug-files-join")
			.action(Arguments.storeTrue())
			.setDefault(false)
			.help("join debug files output into one file (by default every component logs to different file)");
	
		
		logArgGroup.addArgument("--stat")
			.action(Arguments.storeTrue())
			.setDefault(false)
			.help("write statistics to info log");
		
		/** EVENT TYPES and ESPER RUNTIME */
		ArgumentGroup eventArgGroup = parser.addArgumentGroup("Event types & proccessing configuration");
		eventArgGroup.addArgument("-l")
			.action(Arguments.storeTrue())
			.metavar("list")
			.help("list registered event types and exit");
		
		eventArgGroup.addArgument("-L")
			.action(Arguments.storeTrue())
			.metavar("extended-list")
			.help("list registered event types with their fields and exit");
		
		eventArgGroup.addArgument("-e")
			.type(String.class)
			.metavar("event")
			.action(Arguments.append())
			.help("specify event types to use (multiple arguments can be used). Only specified types (and parent types) will be parsed and queryable by esper.");
		
		eventArgGroup.addArgument("--clock")
			.choices(Arrays.asList("internal", "external"))
			.help("Force the use of internal (system time) or external (custom time) clock usage. By default stdin input use internal and file input uses external");
		
		eventArgGroup.addArgument("-p")
			.type(String.class)
			.action(Arguments.append())
			.metavar("pattern-file")
			.help("set custom patterns (pattern as string or path to file); arg can be used multiple times; if not set, default patternset is used");
		
		eventArgGroup.addArgument("--no-patternset")
			.type(Boolean.class)
			.action(Arguments.storeTrue())
			.help("skip loading default patternset (except necessary base patterns). this is default action if custom pattern given.");
		
		eventArgGroup.addArgument("-k")
			.action(Arguments.storeTrue())
			.help("keep list of all matched event types in __match field");
		
		eventArgGroup.addArgument("--remove-raw-data")
			.action(Arguments.storeTrue())
			.help("remove 'data' field before output. this field usually contains raw data leftovers.");
		

		ArgumentGroup inputArgGroup = parser.addArgumentGroup("Data input").description("Select data input type. Multiple inputs can be selected together but how they operate depends on the input.");
		inputArgGroup.addArgument("--file")
			.type(String.class)
			.action(Arguments.append())
			.help("use file(s) as data input. multiple arguments for multiple files can be used");
		
		inputArgGroup.addArgument("--tail")
			.type(Boolean.class)
			.action(Arguments.storeTrue())
			.setDefault(false)
			.help("tail files instead of just reading from start to end (only applicable for --file, not --repeat-file)");
		
		inputArgGroup.addArgument("--repeat-file")
			.type(String.class)
			.action(Arguments.append())
			.help("use file(s) as data input. continue to read repeatedly in every {repeat-file-interval} seconds. multiple arguments for multiple files can be used");

		inputArgGroup.addArgument("--repeat-file-interval")
			.type(Integer.class)
			.setDefault(0)
			.help("repeat file read for every n seconds. default is 0 (disabled), so this must be explicitly set every time");

		inputArgGroup.addArgument("--stdin")
			.action(Arguments.storeTrue())
			.help("use stdin as data input. lines can be raw data or paths to files. is is the default data input");
		
		
		ArgumentGroup outputArgGroup = parser.addArgumentGroup("Output").description("Select one or more output types. Default output is json encoded events to stdout");
		outputArgGroup.addArgument("--stdout")
			.type(Boolean.class)
			.setDefault(false)
			.action(Arguments.storeTrue())
			.metavar("stdout")
			.help("force output explicitly to stdout");
		
		outputArgGroup.addArgument("--out-file")
			.type(String.class)
			.metavar("file")
			.help("where to output results. '-' is stdout and it's the default output. Value 'file' has special meaning also - {instance-name} + '.out' is used as output file name");

		ArgumentGroup fileArgGroup = parser.addArgumentGroup("File (or stdout) output configuration");
		fileArgGroup.addArgument("--out-format")
			.type(String.class)
			.metavar("format")
			.help("format output using apache-commons-text StringSubstitutor in default configuration. {format} can be path to file or raw format as string. This applies to stdout & file output.");
		
		ArgumentGroup testHelpArgGroup = parser.addArgumentGroup("Testdata");
		testHelpArgGroup.addArgument("--test")
			.action(Arguments.storeTrue())
			.help("use built-in default testquery and testdata (both can be overridden)");
		
		parser.addArgument("query")
			.metavar("query")
			.nargs("*")
			//.setDefault(Arrays.asList("select * from events"))
			.type(String.class)
			.help("esper EPL query or path to epl query file (def: " + DEFAULT_EPL_STATEMENT + ")");
		
		Namespace pargs = new Namespace(null);
		try {
            pargs = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

		initializeLogging();

		if(pargs.getString("name") != null) {
			configuration.applicationName = pargs.getString("name"); 
			CometApplication.appName = configuration.applicationName;
		} else {
			CometApplication.appName = configuration.applicationName;
		}
		
		if(pargs.getString("basepath") != null) {
			configuration.basepath = pargs.getString("basepath");
		}
		

		if(pargs.getBoolean("debug_esper") != null && pargs.getBoolean("debug_esper")) {
			configuration.logConfiguration.debugComponents.add(LOG_COMPONENT_ESPER);
		}
		
		if(pargs.getBoolean("debug_enable_all") != null && pargs.getBoolean("debug_enable_all")) {
			configuration.logConfiguration.debugComponents = LOG_COMPONENTS;
		}
		
		if(pargs.getBoolean("debug_files_join") != null && pargs.getBoolean("debug_files_join")) {
			configuration.logConfiguration.debugLogsSeparate = !pargs.getBoolean("debug_files_join");
		}

		if (pargs.getString("log_path") != null && pargs.getString("log_path").trim().length() > 0) {
			configuration.logConfiguration.logPath = pargs.getString("log_path");
		}

		if(pargs.getString("log") != null && pargs.getString("log").length() > 0) {
			configuration.logConfiguration.infoEnabled = true;
			configuration.logConfiguration.infoFile = pargs.getString("log");
			configuration.logConfiguration.hasBeenConfigured = true;
		}

		if(pargs.getBoolean("debug") != null && pargs.getBoolean("debug")) {
			configuration.logConfiguration.debugEnabled = true;
			configuration.logConfiguration.hasBeenConfigured = true;
			if(configuration.logConfiguration.infoEnabled && !configuration.logConfiguration.isInfoStdout()) {
				configuration.logConfiguration.debugFile = "debug-" + configuration.logConfiguration.infoFile;
			} else {
				configuration.logConfiguration.debugFile = "-";
			}
		}
		
		if(pargs.getBoolean("interactive")) {
			
			// if logging wasn't enabled before, then turn the default level to error
			if(!configuration.logConfiguration.infoEnabled) {
				configuration.logConfiguration.defaultLevel = Level.ERROR;
				configuration.logConfiguration.infoFile = "-";
			}

			configuration.logConfiguration.infoEnabled = true;
			configuration.logConfiguration.coloredFatal = true;
		}
		
		/** CONFIGURE LOGGING **/
		if(configuration.logConfiguration.isEnabled()) {
			configureLogging(configuration.logConfiguration);
			log = LogManager.getLogger(CometApplication.class);
			log.debug("configured debug logging with components: {}", configuration.logConfiguration.debugComponents);
		} else {
			log = LogManager.getLogger("NOOPLogger");
		}
		
		log.info("Initializing comet version {}", getApplicationVersion());
		
		if(pargs.getBoolean("help")) {
			log.info("Print help and exit");
			parser.printHelp();
			System.out.println("\n");
			System.out.println("Run -H (--extended-help) too see more ...");
			System.exit(0);
		}
		
		if(pargs.getBoolean("extended_help")) {
			log.info("Print extended help and exit");
			parser.printHelp();
			System.out.println("\n");
			System.out.println(extendedHelp);
			System.exit(0);
		}

		if(pargs.getBoolean("version")) {
			System.out.println(this.getApplicationVersion());
			System.exit(0);
		}

		if(pargs.getBoolean("validate")) {
			this.validationMode = true;
		}

		
		/** READ IN RUNTIME CONFIGURATIONS **/

		CometConfigurationCmdlineArg.parseApplicationConfiguration(pargs, this, configuration);
		
		
		if(pargs.getList("c") != null && pargs.getList("c").size() > 0) {
			
			for(Object confFile : pargs.getList("c")) {
				try {
					// we can't use getBasepath() here yet
					File runtimeConfFile = new File((String)confFile);
					if(!((String)confFile).startsWith("/")) {
						runtimeConfFile = new File(configuration.basepath + File.separator + (String)confFile);
					}

					MonitorRuntime monitor = loadMonitorRuntime(runtimeConfFile);
					if(monitor != null) {
						monitor.start();
						DEFAULT_MONITOR_ID = monitor.configuration.runtimeName;
						
						if(pargs.getList("c").size() == 1) {
							
							/// TODO! this application renaming doesn't work quite like that, have to rethink it
							/*if(!configuration.applicationName.equals(monitor.configuration.runtimeName)) {
								Path link = new File(getLogSocketPath(configuration.applicationName)).toPath();
								Path target = new File(getLogSocketPath(monitor.configuration.runtimeName)).toPath();
								Files.createSymbolicLink(link, target);
							}
							configuration.applicationName = monitor.configuration.runtimeName;*/
						}
					}
					
				} catch(Exception e) {
					log.error("failed loading monitor '{}', because: {}", (String)confFile, e.getMessage());
					log.debug("failed loading monitor '{}', because: {}", (String)confFile, e.getMessage(), e);
				}
			};
			
			log = LogManager.getLogger(CometApplication.class);
		} else {
			// a bit hackis that this is configured here.. 
			
			MonitorRuntimeConfiguration monitorConf = new MonitorRuntimeConfiguration(configuration);
			CometConfigurationCmdlineArg.parseRuntimeConfiguration(pargs, this, monitorConf);
			if(monitorConf.runtimeName.equals(MonitorRuntimeConfiguration.RUNTIME_NAME_UNSET)) {
				monitorConf.runtimeName = configuration.applicationName;
			}
			
			MonitorRuntime monitor = loadMonitorRuntime(monitorConf);
			if(monitor != null) {
				monitor.start();
				DEFAULT_MONITOR_ID = monitorConf.runtimeName;
			}
		}
		
		// parse external configuration file

		
		// we should be all set up now, so list events and exit if needed
		if(pargs.getBoolean("l") || pargs.getBoolean("L")) {
			log.info("print event types and exit");
			// TODO! - how?
			System.exit(0);
		}
		
	}

	/*** secrets management ***/

	public static Map<String, Object> getSecret(String secret) {
		Map<String, Map<String, Object>> secrets = getSecrets();
		if(secrets.containsKey(secret)) {
			return secrets.get(secret);
		}
		return null;
	}

	public static Map<String, Object> getSecret(File secretsFile, String secret) {
		Map<String, Map<String, Object>> secrets = getSecrets(secretsFile);
		if(secrets.containsKey(secret)) {
			return secrets.get(secret);
		}
		return null;
	}

	public static Map<String, Map<String, Object>> getSecrets() {
		return getSecrets(null);
	}
	
	public static Map<String, Map<String, Object>> getSecrets(File secretsFile) {
		
		Map<String, Map<String, Object>> secrets = new HashMap<String, Map<String, Object>>();

		/** get from java runtime dir (one level up from classpath - so next to jar) **/
		try {
			Map<String, Map<String, Object>> runPathSecrets = getSecretsFromClasspath();
			secrets.putAll(runPathSecrets);
		} catch(Exception e) {
			// pass
		}
		
		/** get from user home dir **/
		try {
			Map<String, Map<String, Object>> homeSecrets = getSecretsFromUserHome();
			secrets.putAll(homeSecrets);
		} catch (Exception e) {
			// pass
		}
		
		/** get from execution dir **/
		try {
			Map<String, Map<String, Object>> execSecrets = getSecretsFromExecutionPath();
			secrets.putAll(execSecrets);
		} catch (Exception e) {
			// pass
		}
		
		if(secretsFile != null) {
			
			/** get from custom dir (configured?) **/
			try {
				Map<String, Map<String, Object>> execSecrets = getSecretsFromFile(secretsFile);
				secrets.putAll(execSecrets);
			} catch (Exception e) {
				// pass
			}			
		}
		
		return secrets;
	}

	private static Map<String, Map<String, Object>> getSecretsFromClasspath() throws Exception {
		return getSecretsFromFile(new File(System.getProperty("java.class.path")).getParentFile().getAbsolutePath()  + File.separator + "secrets.yaml");
	}
	
	private static Map<String, Map<String, Object>> getSecretsFromFile(String fileName) throws Exception {
		return getSecretsFromFile(new File(fileName));
	}
	
	private static Map<String, Map<String, Object>> getSecretsFromUserHome() throws Exception {
		File homePath = new File(System.getProperty("user.home"));
		File secretsFile = new File(homePath.getAbsolutePath() + File.separator + ".comet/secrets.yaml");
		return getSecretsFromFile(secretsFile);
	}
	
	private static Map<String, Map<String, Object>> getSecretsFromExecutionPath() throws Exception {
		File execPath = new File(getBasepath());
		File secretsFile = new File(execPath.getAbsolutePath() + File.separator + "secrets.yaml");
		return getSecretsFromFile(secretsFile);
	}
	
	/**
	 * if --basepath is set, it returns this. if not set, this defaults to system(user.dir)
	 * 
	 * @return
	 */
	public static String getBasepath() {
		if(getApplication() == null) {
			return System.getProperty("user.dir");
		} else {
			return getApplication().configuration.basepath;
		}
	}
	
	private static Map<String, Map<String, Object>> getSecretsFromFile(File secretsFile) throws Exception {
		Yaml yaml = new Yaml();
		
		Map<String, Map<String, Object>> configurationObj = new HashMap<String, Map<String, Object>>();
		
		if(secretsFile.exists() && secretsFile.isFile()) {
			InputStream inputStream = null;
			try {
				inputStream = new FileInputStream(secretsFile);
			} catch (FileNotFoundException e) {
				// just pass
			}
			
			try {
				if(inputStream != null) {
					configurationObj = yaml.load(inputStream);
				}
			} catch(ClassCastException e) {
				//log.debug("yaml isn't a map of maps, but it has to be");
				throw new Exception("yaml isn't a map of maps, but it has to be", e);
			}
			
			LogManager.getLogger(CometApplication.class).debug("Found secrets file candidate from '{}'", secretsFile.getAbsoluteFile());
		}
		return configurationObj;
	}

	
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
	
	public String getApplicationVersion() {
		Properties props = getApplicationProperties();
		return props.getProperty("version", "DEV");
	}
	
	
	/** logging configuration ***/
	
	static LoggerContext initializeLogging() {

		ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();

		// create root logger and set it to off
		RootLoggerComponentBuilder rootLogger   = builder.newRootLogger(Level.OFF);
		builder.add(rootLogger);
		return Configurator.initialize(builder.build());
	}
	
	static void configureLogging(CometApplicationConfiguration.LogConfiguration logConf) {
		
		Map<String, String> logComponentsClasses = new HashMap<String, String>(){{
			put(LOG_COMPONENT_COMET, "com.github.arikastarvo.comet");
			put(LOG_COMPONENT_ESPER, "com.espertech");
		}};
		
		Map<String, Level> logComponentsDefaultLevels = new HashMap<String, Level>(){{
			//
		}};
		
		
		
		if(logConf.debugComponents == null) {
			logConf.debugComponents = LOG_COMPONENTS;
		}
		

		ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
		
		String logPattern = "%date{ISO8601}\\t%threadName\\t%level\\t%logger{2}\\t%equals{%X{monitor}}{}{-}\\t%ex{0}%replace{%message}{\\n}{}%n";

		if(logConf.debugEnabled && logConf.debugComponents.size() > 0) {
			logPattern = "%date{ISO8601}\\tTRACE\\t%level\\t%logger{2}\\t%equals{%X{monitor}}{}{-}\\t%replace{%message}{\\n}{}%n";
		}


		//String coloredErrPattern = "%highlight{[ERR]}%equals{%X{monitor}}{}{-}\\\\t%replace{%message}{\\\\n}{}%n";
		
		
		//standard.addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable");
		
		AppenderComponentBuilder console = builder.newAppender("stdout", "Console"); 
		LayoutComponentBuilder stdoutPattern  = builder.newLayout("PatternLayout");
		stdoutPattern.addAttribute("pattern", logPattern);
		console.add(stdoutPattern);
		//console.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.DENY).addAttribute("level", Level.INFO));
		//console.add(builder.newFilter("ThresholdFilter", Filter.Result.DENY, Filter.Result.NEUTRAL).addAttribute("level", Level.DEBUG));
		builder.add(console);

		/*if(logConf.debugEnabled && logConf.debugComponents.size() > 0) {
			System.out.println(logConf.debugEnabled);
			System.out.println(logConf.debugComponents);

			AppenderComponentBuilder consoleWithException = builder.newAppender("stdout-exception", "Console");
			LayoutComponentBuilder stdoutPatternWithException = builder.newLayout("PatternLayout");
			stdoutPatternWithException.addAttribute("pattern", logPatternWithException);
			consoleWithException.add(stdoutPatternWithException);
			consoleWithException.add(builder.newFilter("ThresholdFilter", Filter.Result.DENY, Filter.Result.NEUTRAL).addAttribute("level", Level.INFO));
			consoleWithException.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.DENY).addAttribute("level", Level.DEBUG));
			builder.add(consoleWithException);
		}*/
	
		builder.setPackages("com.github.arikastarvo.comet");
		
		// create root logger and set it to off
		RootLoggerComponentBuilder rootLogger = builder.newRootLogger(Level.OFF);
		builder.add(rootLogger);
		

		List<String> appenders = new ArrayList<String>();
		
		if(logConf.socketEnabled) {
			File sockFile = new File(getLogSocketPath(CometApplication.getApplicationName()));
			builder.add(getSocketAppender(builder, "socket", true));
			appenders.add("socket");
		}
		
		for(String logComponent : LOG_COMPONENTS) {

			boolean debugMe = logConf.debugEnabled && logConf.debugComponents.contains(logComponent);
			
			String infoLogger = null;
			if(logConf.infoEnabled && logConf.infoFile != null && logConf.isInfoStdout()) {
				infoLogger = "stdout";
			} else if (logConf.infoEnabled && logConf.infoFile != null && logConf.infoFile.length() > 0) {
				infoLogger = "file";
			}
			
			String debugLogger = null;
			if(debugMe) {
				if (!logConf.isDebugStdout() && logConf.debugFile != null && logConf.debugFile.length() > 0 && !logConf.debugLogsSeparate) {
					debugLogger = "debugfile";
				} else if (!logConf.isDebugStdout() && logConf.debugFile != null && logConf.debugFile.length() > 0 && logConf.debugLogsSeparate) {
					debugLogger = "debugfile-" + logComponent;
				} else {
					debugLogger = "stdout";
				}
			}
			
			if(logConf.infoEnabled && !logConf.isInfoStdout() && !appenders.contains(infoLogger)){
				builder.add(getRollingFile(builder, infoLogger, logConf.getFullInfoPath(), false));
				appenders.add(infoLogger);
			}
			
			if(debugMe && !logConf.isDebugStdout() && !appenders.contains(debugLogger)){
				if(logConf.debugLogsSeparate) {
					builder.add(getRollingFile(builder, debugLogger, logConf.getFullDebugPath(logComponent), true));
				} else {
					builder.add(getRollingFile(builder, debugLogger, logConf.getFullDebugPath(), true));
				}
				appenders.add(debugLogger);
			}
			
			Level defaultLevel = logComponentsDefaultLevels.containsKey(logComponent)?logComponentsDefaultLevels.get(logComponent):logConf.defaultLevel;
			LoggerComponentBuilder logger = builder.newLogger(logComponentsClasses.get(logComponent), debugMe?Level.DEBUG:defaultLevel);
			
			logger.addAttribute("additivity", false);
			if(logConf.infoEnabled) {
				logger.add(builder.newAppenderRef(infoLogger));
				if(logConf.socketEnabled) {
					logger.add(builder.newAppenderRef("socket"));
				}
				if(debugMe) {
					logger.add(builder.newAppenderRef(debugLogger));
				}
				if(logConf.coloredFatal) {
					logger.add(builder.newAppenderRef("fatalStderr"));
				}
			}
			builder.add(logger);
		}
		
		if(logConf.coloredFatal) {
			// add appender for fatal level to stderr
			builder.add(getFatalStderr(builder));
		}

		// i think this disable shutdown hook here doesn't work (jvm cmd line arg does the job better). but i'll leave it here for now;
		builder.setShutdownHook("disable");
		Configurator.reconfigure(builder.build());
		//System.out.println(builder.toXmlConfiguration());
	}
	
	public String getLogSocketPath() {
		return CometApplication.getLogSocketPath(configuration.applicationName);
	}

	private static String getLogSocketPath(String name) {
		return "/tmp/monitord." + CometApplication.getApplicationName() + ".log.sock";
	}
	

	private static AppenderComponentBuilder getFatalStderr(ConfigurationBuilder<BuiltConfiguration> builder) {
		String logPattern = "%highlight{[ERR]} %ex{0}%replace{%message}{\\\\n}{}%n";
		
		AppenderComponentBuilder fatalStderrAppender = builder.newAppender("fatalStderr", "Console");
		fatalStderrAppender.addAttribute("target", "SYSTEM_ERR");
		fatalStderrAppender.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.DENY).addAttribute("level", Level.ERROR));

		LayoutComponentBuilder filePattern  = builder.newLayout("PatternLayout");
		filePattern.addAttribute("pattern", logPattern);
		fatalStderrAppender.add(filePattern);
		
		return fatalStderrAppender;
	}
	
	private static AppenderComponentBuilder getSocketAppender(ConfigurationBuilder<BuiltConfiguration> builder, String name, boolean debug) {
		String logPattern = "%date{ISO8601}\\t%threadName\\t%level\\t%logger{2}\\t%equals{%X{monitor}}{}{-}\\t%ex{0}%replace{%message}{\\n}{}%n";
		String logPatternWithException = "%date{ISO8601}\\t%threadName\\t%level\\t%logger{2}\\t%equals{%X{monitor}}{}{-}\\t%replace{%message}{\\n}{}%n";
		
		AppenderComponentBuilder socketAppender = builder.newAppender(name, "Log4j2UnixNamedSocketAppender");
		socketAppender.addAttribute("socket", getLogSocketPath(CometApplication.getApplicationName()));
		if(!debug) {
			socketAppender.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.DENY).addAttribute("level", Level.INFO));
		}
		/*
		 * this solution kept trace -> debug in debug and error -> info in info file (no overlap)
		 * if(debug) {
			rollingFile.add(builder.newFilter("ThresholdFilter", Filter.Result.DENY, Filter.Result.ACCEPT).addAttribute("level", Level.INFO));
		} else {
			rollingFile.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.DENY).addAttribute("level", Level.INFO));
		}*/
		
		LayoutComponentBuilder socketPattern  = builder.newLayout("PatternLayout");
		socketPattern.addAttribute("pattern", debug?logPatternWithException:logPattern);
		socketAppender.add(socketPattern);
		return socketAppender;
	}
	
	private static AppenderComponentBuilder getRollingFile(ConfigurationBuilder<BuiltConfiguration> builder, String name, String path, boolean debug) {
		String logPattern = "%date{ISO8601}\\t%threadName\\t%level\\t%logger{2}\\t%equals{%X{monitor}}{}{-}\\t%ex{0}%replace{%message}{\\n}{}%n";
		String logPatternWithException = "%date{ISO8601}\\t%threadName\\t%level\\t%logger{2}\\t%equals{%X{monitor}}{}{-}\\t%replace{%message}{\\n}{}%n";
		
		AppenderComponentBuilder rollingFile = builder.newAppender(name, "File");
		rollingFile.addAttribute("fileName", path);
		if(!debug) {
			rollingFile.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.DENY).addAttribute("level", Level.INFO));
		}
		/*
		 * this solution kept trace -> debug in debug and error -> info in info file (no overlap)
		 * if(debug) {
			rollingFile.add(builder.newFilter("ThresholdFilter", Filter.Result.DENY, Filter.Result.ACCEPT).addAttribute("level", Level.INFO));
		} else {
			rollingFile.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.DENY).addAttribute("level", Level.INFO));
		}*/
		LayoutComponentBuilder filePattern  = builder.newLayout("PatternLayout");
		filePattern.addAttribute("pattern", debug?logPatternWithException:logPattern);
		rollingFile.add(filePattern);
		return rollingFile;
	}
	
	public void testLogging() {
		log.info("test log entry");
	}
	
	public void Stop() {
		
		for(MonitorRuntime monitor : monitors.values()) {
			if(monitor.running) {
				monitor.Stop();
			}
		}
		
	}

	public MonitorRuntime getMonitorRuntime() {
		return monitors.get(this.DEFAULT_MONITOR_ID);
	}
	
	public MonitorRuntime getMonitorRuntime(String id) {
		return monitors.get(id);
	}
	
	public Map<String, MonitorRuntime> getMonitorRuntimes() {
		return monitors;
	}
	
	/**
	 * TODO! we must have a generic method for adding (+ pausing, resuming, removing etc) new monitors
	 * 
	 */
	public MonitorRuntime loadMonitorRuntime(File confFile) throws Exception {
		
		if(confFile.exists() && confFile.isFile()) {
			log.debug("starting to parse configuration for monitor '{}'", confFile.getAbsolutePath());
			MonitorRuntimeConfiguration monitorConf = new MonitorRuntimeConfiguration(configuration);
			CometConfigurationYaml.parseConfiguration(confFile.getAbsolutePath(), this, monitorConf);

			MonitorRuntime monitor = loadMonitorRuntime(monitorConf);
			monitorsConfigurationFiles.put(monitorConf.runtimeName, confFile);
			return monitor;
		} else {
			throw new Exception("no monitor configuration file found at: " + confFile.getAbsolutePath());
		}
	}
	
	private MonitorRuntime loadMonitorRuntime(MonitorRuntimeConfiguration monitorConf) throws Exception {
		// don't override command line args here?
		
		if (monitors.containsKey(monitorConf.runtimeName)) {
			throw new Exception("monitor with name '" + monitorConf.runtimeName +"' already exists");
		}
		MonitorRuntime monitor = new MonitorRuntimeEsperImpl(monitorConf);
		if(configuration.daemonize && !monitor.configuration.hasInputs()) {
			log.debug("Adding dummy input to '{}' for daemonized run", monitor.configuration.runtimeName);
			monitor.addDummyInput();
		}
		
		monitors.put(monitorConf.runtimeName, monitor);
		
		return monitor;
	}
	
	public void unloadMonitorRuntime(String monitorRuntime) throws Exception {

		if(monitors.containsKey(monitorRuntime)) {
			if(monitors.get(monitorRuntime).running) {
				log.info("Stopping monitor '{}'", monitorRuntime);
				monitors.get(monitorRuntime).Stop();
			}
			log.info("unloading monitor '{}'", monitorRuntime);
			monitors.remove(monitorRuntime);
			if(monitorsConfigurationFiles.containsKey(monitorRuntime)) {
				monitorsConfigurationFiles.remove(monitorRuntime);
			}
			
			if(monitors.size() == 0) {
				log.debug("last monitor was unloaded, i'm shutting down now...bye");
				Stop();
			}
		} else {
			throw new Exception("no such monitor");
		}
	}
	
	public void reloadMonitorRuntime(String monitorRuntime) throws Exception {

		if(monitors.containsKey(monitorRuntime)) {
			Boolean wasRunning = monitors.get(monitorRuntime).running;
			if(wasRunning) {
				log.info("Stopping monitor '{}'", monitorRuntime);
				monitors.get(monitorRuntime).Stop();
			}
			log.info("unloading monitor '{}'", monitorRuntime);
			if(monitorsConfigurationFiles.containsKey(monitorRuntime)) {
				File conf = monitorsConfigurationFiles.get(monitorRuntime);
				monitors.remove(monitorRuntime);
				monitorsConfigurationFiles.remove(monitorRuntime);
				MonitorRuntime monitor = loadMonitorRuntime(conf);
				if(wasRunning) {
					monitor.start();
				}
			}
		} else {
			throw new Exception("no such monitor");
		}
	}
	
	public void startMonitorRuntime(String monitorRuntime) throws Exception {
		if(monitors.containsKey(monitorRuntime)) {
			if(monitors.get(monitorRuntime).running) {
				log.debug("tried to start already running monitor '{}'", monitorRuntime);
				throw new Exception("monitor already running");
			} else {
				if(monitors.get(monitorRuntime).dirty) {
					
					// re-load and re-create conf ( we can't restart old thread)
					if(monitorsConfigurationFiles.containsKey(monitorRuntime)) {
						File conf = monitorsConfigurationFiles.get(monitorRuntime);
						unloadMonitorRuntime(monitorRuntime);
						MonitorRuntime monitor = loadMonitorRuntime(conf);
						monitor.start();
					} else {
						log.error("tried to reload configuration to restart monitor '{}' but no old configuration reference was found", monitorRuntime);
						throw new Exception("could not load monitor, no configuration found");
					}
				} else {
					log.info("starting monitor '{}'", monitorRuntime);
					monitors.get(monitorRuntime).start();
				}
			}
		} else {
			throw new Exception("no such monitor");
		}
	}
	
	public void stopMonitorRuntime(String monitorRuntime) throws Exception {
		if(monitors.containsKey(monitorRuntime)) {
			if(monitors.get(monitorRuntime).running) {
				log.info("stopping monitor '{}'", monitorRuntime);
				monitors.get(monitorRuntime).Stop();
			} else {
				log.debug("tried to stop already stopped monitor '{}'", monitorRuntime);
				throw new Exception("monitor already stopped");
			}
		} else {
			throw new Exception("no such monitor");
		}
	}
	
	public boolean isMonitorRunning(String monitorRuntime) throws Exception {
		if(monitors.containsKey(monitorRuntime)) {
			return monitors.get(monitorRuntime).running;
		} else {
			throw new Exception("no such monitor");
		}
	}
}
