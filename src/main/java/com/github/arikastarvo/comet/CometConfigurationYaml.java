package com.github.arikastarvo.comet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.InputConfiguration;
import com.github.arikastarvo.comet.input.ReferenceInput;
import com.github.arikastarvo.comet.input.URICapableInputConfiguration;
import com.github.arikastarvo.comet.output.file.FileOutput;
import com.github.arikastarvo.comet.output.file.FileOutputConfiguration;
import com.github.arikastarvo.comet.output.noop.NoopOutput;
import com.github.arikastarvo.comet.output.stdout.StdoutOutputConfiguration;
import com.github.arikastarvo.comet.output.Output;
import com.github.arikastarvo.comet.output.OutputConfiguration;
import com.github.arikastarvo.comet.reference.Reference;
import com.github.arikastarvo.comet.reference.ReferenceReloadCallback;
import com.github.arikastarvo.comet.reference.SQLiteReference;
import com.github.arikastarvo.comet.utils.FileSystem;

public class CometConfigurationYaml {
	
	static Logger log = LogManager.getLogger(CometConfigurationYaml.class);
	
	static CometApplication app;
	static MonitorRuntimeConfiguration conf;

	private Map<String,String> namedInputs = new HashMap<String, String>();
	private Map<String,String> namedOutputs = new HashMap<String, String>();
	
	public static void parseConfiguration(String confFilePath, CometApplication appObj, MonitorRuntimeConfiguration confObj) {
		app = appObj;
		conf = confObj;
		
		//String parentPath;
		
		File confFile = new File(confFilePath);
		log.debug("starting to read/parse configuration file " + confFile);
		
		if(!confFile.exists()) {
			log.warn("No configuration file found at '" + confFilePath + "'");
		} else {
			
			conf.configurationSource = confFile.getAbsolutePath();

			conf.sourceConfigurationPath = new File(confFile.getAbsolutePath()).getParent();

			Yaml yaml = new Yaml();
			InputStream inputStream = null;
			try {
				inputStream = new FileInputStream(confFile);
			} catch (FileNotFoundException e) {
				log.warn("No configuration file found at '" + confFilePath + "'");
			}
			
			// if secrets file hasn't been set by command line args (command line args override config file), then search for locations related to config file
			if(conf.secretsPath == null) {
				// look for monitor specific secrets file in the default location
				File monitorSecFileLoc = new File(new File(confFile.getAbsolutePath()).getParentFile().getParentFile().getParentFile().getAbsolutePath() + File.separator + "config" + File.separator + "secrets.yaml");
				if(monitorSecFileLoc.exists() && monitorSecFileLoc.isFile()) {
					conf.secretsPath = monitorSecFileLoc.getAbsolutePath();
				}
				
				// look for secrets file next to the config file
				File secFileLoc = new File(new File(confFile.getAbsolutePath()).getParentFile().getAbsolutePath() + File.separator + "secrets.yaml");
				if(secFileLoc.exists() && secFileLoc.isFile()) {
					conf.secretsPath = secFileLoc.getAbsolutePath();
				}
			}
			
			// set default conf name
			List<String> tokens = Arrays.asList(confFile.getName().split("\\."));
			conf.runtimeName = String.join(".", tokens.subList(0, tokens.size() - 1));
			
			Map<String, Object> configurationObj = null;
			try {
				configurationObj = yaml.load(inputStream);
			} catch(Exception e) {
				log.warn("Yaml error during configuration loading: " + e.getMessage());
				log.debug("Yaml error during configuration loading: " + e.getMessage(), e);
				return;
			}
			
			// hacki crapi
			CometConfigurationYaml co = new CometConfigurationYaml();
			co.parseConfigurationObject(configurationObj, conf);
		}
	}
		
	private MonitorRuntimeConfiguration parseConfigurationObject(Map<String, Object> configurationObj, MonitorRuntimeConfiguration conf) {		
		
		if(conf == null) {
			conf = new MonitorRuntimeConfiguration();
		}
		
		if(configurationObj != null) {

			/**
			 *
			 * Profile - GENERAL config
			 *
			 */

			if(configurationObj.containsKey("profile")) {
				try {
					for (Map.Entry<String, Map<String, Object>> entry : ((Map<String, Map<String, Object>>) configurationObj.get("profile")).entrySet()) {

						if (conf.applicationConfiguration.profiles.contains(entry.getKey())) {

							log.debug("enabling general configs for profile {}", entry.getKey());
							Map<String, Object> profileConf = entry.getValue();
							Arrays.asList(new String[]{"log", "debug", "log-path", "debug-enable"}).forEach( confItem -> {
								if(profileConf.containsKey(confItem)) {
									configurationObj.put(confItem, profileConf.get(confItem));
								}
							});
						}
					}
				} catch(Exception e) {
					log.error("parsing profiles failed for some reason: {}", e.getMessage(), e);
				}
			}
			if(configurationObj.containsKey("name")) {
				conf.runtimeName = configurationObj.get("name").toString();
			}
			
			if(configurationObj.containsKey("locked") && configurationObj.get("locked") instanceof Boolean && ((Boolean)configurationObj.get("locked"))) {
				conf.locked = true;
			}
			
			if(configurationObj.containsKey("clock") && configurationObj.get("clock") instanceof String) {
				if (((String)configurationObj.get("clock")).equals("external")) {
					conf.externalClock = true;
				}
			}
			
			if(configurationObj.containsKey("initial-time") && configurationObj.get("initial-time") instanceof Long) {
				conf.initialTime = (Long)configurationObj.get("initial-time");
			} else if(configurationObj.containsKey("initial-time") && configurationObj.get("initial-time") instanceof Integer) {
				conf.initialTime = ((Integer)configurationObj.get("initial-time")).longValue();
			}
			
			
			if(configurationObj.containsKey("log") || configurationObj.containsKey("debug")) {
				// only allow to configure info level logging if it hasn't been configured before
				if(!app.configuration.logConfiguration.hasBeenConfigured && configurationObj.containsKey("log") && configurationObj.get("log") != null) {

					if(configurationObj.get("log") instanceof String) {
						app.configuration.logConfiguration.infoEnabled = true;
						app.configuration.logConfiguration.hasBeenConfigured = true;
						String info = (String) configurationObj.get("log");
						if (info.equals("-") || info.equals("stdout")) {
							app.configuration.logConfiguration.infoFile = "-";
						} else if (!info.equals("file")) {
							app.configuration.logConfiguration.infoFile = info;
						}
					} else if (configurationObj.get("log") instanceof Boolean && (Boolean)configurationObj.get("log")) {
						app.configuration.logConfiguration.infoEnabled = true;
						app.configuration.logConfiguration.hasBeenConfigured = true;
						app.configuration.logConfiguration.infoFile = "-";
					}
				}
				
				// only allow to configure debug if not configured before or debug is false
				if((!app.configuration.logConfiguration.hasBeenConfigured || !app.configuration.logConfiguration.debugEnabled) && configurationObj.containsKey("debug") && configurationObj.get("debug") != null) {

					if(configurationObj.get("debug") instanceof String) {

						app.configuration.logConfiguration.debugEnabled = true;
						app.configuration.logConfiguration.hasBeenConfigured = true;
						app.configuration.logConfiguration.infoEnabled = true;

						String debug = (String) configurationObj.get("debug");
						if (debug.equals("-") || debug.equals("stdout")) {
							app.configuration.logConfiguration.debugFile = "-";
						} else if (!debug.equals("file")) {
							app.configuration.logConfiguration.debugFile = debug;
						}
					} else if(configurationObj.get("debug") instanceof Boolean && (Boolean)configurationObj.get("debug")) {
						app.configuration.logConfiguration.debugEnabled = true;
						app.configuration.logConfiguration.hasBeenConfigured = true;
						app.configuration.logConfiguration.infoEnabled = true;
						app.configuration.logConfiguration.debugFile = "-";
					}
				}
				
				// WE ONLY SET logPath if it wasn't set before by command-line-args (so command-line args override config file)
				if(configurationObj.containsKey("log-path") && configurationObj.get("log-path") != null && app.configuration.logConfiguration.logPath == null) {
					String path = (String)configurationObj.get("log-path");
					if(!path.startsWith("/")) { // we have relative path, so prefix it with conf file path
						path = conf.sourceConfigurationPath + File.separator + path;
					}
					app.configuration.logConfiguration.logPath = path;
				}

				if(configurationObj.containsKey("debug-enable") && configurationObj.get("debug-enable") != null) {
					if(configurationObj.get("debug-enable") instanceof String) {
						app.configuration.logConfiguration.debugComponents = Arrays.asList((String)configurationObj.get("debug-enable"));
					} else if (configurationObj.get("debug-enable") instanceof List) {
						app.configuration.logConfiguration.debugComponents = (List<String>)configurationObj.get("debug-enable");
					}
				}
				if(app.configuration.logConfiguration.logPath != null) {
					new File(app.configuration.logConfiguration.logPath).mkdirs();
				}
				
				/** CONFIGURE LOGGING **/
				CometApplication.configureLogging(app.configuration.logConfiguration);
				log = LogManager.getLogger(CometConfigurationYaml.class);
				log.info("Reconfiguring logging based on external configuration. Comet startup already in progress.");
				log.debug("Debug logging enabled for {}", app.configuration.logConfiguration.debugComponents);
			}
			
			
			/** general configuration **/
			if(configurationObj.containsKey("keep") && configurationObj.get("keep") instanceof Boolean && ((Boolean)configurationObj.get("keep"))) {
				conf.keepMatches = true;
			}
			
			if(configurationObj.containsKey("pattern")) {
				PatternConfigurationContainer pcc = parsePatternConfiguration(configurationObj.get("pattern"));
				conf.addPatternReference(pcc.patternReferences);
				conf.addPattern(pcc.patternDefinitions);
			}
			
			if(configurationObj.containsKey("nopatternset") && configurationObj.get("nopatternset") instanceof Boolean && ((Boolean)configurationObj.get("nopatternset"))) {
				conf.usePatternset = false;
			}

			if(conf.getPatterns().size() > 0) {
				conf.usePatternset = false;
			}
			
			if(configurationObj.containsKey("event-types")) {
				List<String> eventTypes = new ArrayList<String>();
				if(configurationObj.get("event-types") instanceof String) {
					eventTypes.add((String)configurationObj.get("event-types"));
				} else if(configurationObj.get("event-types") instanceof List && ((List)configurationObj.get("event-types")).size() > 0 && ((List)configurationObj.get("event-types")).get(0) instanceof String) {
					eventTypes.addAll((List<String>)configurationObj.get("event-types"));
				} else {
					log.error("only string or list of strings is allowed for event-types: property in config");
				}
			}

			/** end general configuration **/


			/**
			 *
			 * Profile - INPUTS and OUTPUTS
			 *
			 */

			if(configurationObj.containsKey("profile")) {
				try {
					for(Map.Entry<String, Map<String, Object>> entry : ((Map<String, Map<String, Object>>)configurationObj.get("profile")).entrySet()) {

						if(conf.applicationConfiguration.profiles.contains(entry.getKey())) {

							log.debug("enabling inputs and outputs for profile {}", entry.getKey());
							Map<String, Object> profileConf = entry.getValue();

							if(profileConf.containsKey("debug"))

							if(profileConf.containsKey("input")) {
								List<Map<String, Object>> normalizedInputs = normalizeInputConfigurations(profileConf.get("input"));
								//normalizedInputs.stream().filter( it -> it.containsKey("name")).peek( it -> namedInputs.put((String)it.get("name"), (String)it.get("type")));
								List<InputConfigurationContainer> inputConfigurationContainers = parseInputConfigurations(normalizedInputs);
								inputConfigurationContainers.forEach( inputConfigurationContainer -> handleInputConfiguration(inputConfigurationContainer));
							}

							if(profileConf.containsKey("output")) {
								List<Map<String, Object>> normalizedOutputs = normalizeOutputConfigurations(profileConf.get("output"));
								//normalizedOutputs.stream().filter( it -> it.containsKey("name")).peek( it -> namedOutputs.put((String)it.get("name"), (String)it.get("type")));
								List<OutputConfigurationContainer> outputConfigurationContainers = parseOutputConfigurations(normalizedOutputs);
								outputConfigurationContainers.forEach( outputConfigurationContainer -> handleOutputConfiguration(outputConfigurationContainer));
							}
						}
					}
				} catch(Exception e) {
					log.error("parsing profiles failed for some reason: {}", e.getMessage(), e);
				}
			}

			/**
			 *
			 * Persistence
			 *
			 */
			if(configurationObj.containsKey("persistence") && configurationObj.get("persistence") != null) {
				
				if (configurationObj.get("persistence") instanceof String) {
					conf.persistenceConfiguration.persistence.add((String)configurationObj.get("persistence"));
				} else if (configurationObj.get("persistence") instanceof List && ((List)configurationObj.get("persistence")).get(0) instanceof String) {
					conf.persistenceConfiguration.persistence.addAll((List<String>)configurationObj.get("persistence"));
				} else if(configurationObj.get("persistence") instanceof Map) {
					
					Map<String, Object> persistenceConf = ((Map<String, Object>)configurationObj.get("persistence"));
					if (persistenceConf.get("persistence") instanceof String) {
						conf.persistenceConfiguration.persistence.add((String)persistenceConf.get("persistence"));
					} else if (persistenceConf.get("persistence") instanceof List && ((List)persistenceConf.get("persistence")).get(0) instanceof String) {
						conf.persistenceConfiguration.persistence.addAll((List<String>)persistenceConf.get("persistence"));
					} else {
						log.error("no persistence windows defined for persistence configuration");
					}
					
					// here now should be the persist type thing
					
					if (persistenceConf.containsKey("persistence-interval") && persistenceConf.get("persistence-interval") != null) {
						try {
							if(persistenceConf.get("persistence-interval") instanceof Integer) {
								conf.persistenceConfiguration.persistenceInterval = (Integer)persistenceConf.get("persistence-interval");
							} else if (persistenceConf.get("persistence-interval") instanceof String) {
								Float val = Float.parseFloat((String)persistenceConf.get("persistence-interval"));
								conf.persistenceConfiguration.persistenceInterval = Math.round(val);
							} else {
								throw new IllegalArgumentException("not a valid data type");
							}
							
							log.info("set interval to " + conf.persistenceConfiguration.persistenceInterval);
							//Integer.par
						} catch (NumberFormatException e) {
							log.error("invalid non-numeric value '{}' for persistence interval, using default", persistenceConf.get("persistence-interval"));
							log.debug("invalid non-numeric value '{}' for persistence interval, using default", persistenceConf.get("persistence-interval"), e);
						}
					}
					
					try {
						if(persistenceConf.containsKey("storage")) {
							 if(persistenceConf.get("storage") instanceof Map) {
								 Map<String, Object> pStorage = (Map<String, Object>)persistenceConf.get("storage");
								 if(!pStorage.containsKey("type")) {
									 throw new Exception("storage type must be defined");
								 }
								 conf.persistenceConfiguration.storageType = (String)pStorage.get("type");
								 conf.persistenceConfiguration.storageConfiguration = pStorage;
							 } else {
								 throw new Exception("persistence storage configuration must be a map");
							 }
						}
					} catch(Exception e) {
						 log.error("error reading persistence storage configuration: {}", e.getMessage());
					}
				} else {
					log.error("unsupported persistence configuration format");
				}
				
			}
			
			if(configurationObj.containsKey("input")) {

				List<Map<String, Object>> inputDefinitions = normalizeInputConfigurations(configurationObj.get("input"));
				List<InputConfigurationContainer> parsedInputConfigurations = parseInputConfigurations(inputDefinitions);
				for (InputConfigurationContainer inputContainer : parsedInputConfigurations) {
					handleInputConfiguration(inputContainer);
				}
			}
			
			if(configurationObj.containsKey("output")) {

				List<Map<String, Object>> outputDefinitions = normalizeOutputConfigurations(configurationObj.get("output"));
				List<OutputConfigurationContainer> outputList = parseOutputConfigurations(outputDefinitions);
				for (OutputConfigurationContainer outConfContainer : outputList) {
					handleOutputConfiguration(outConfContainer);
				}
			} else {
				//log.debug("adding noop output as the default as nothing else was added via configuration file");
				//conf.addListener("DEFAULT_NOOP_OUTPUT", new EventUpdateListener(new NoopOutput()));
			}
			
			/**
			 * Query parsing
			 */
			if(configurationObj.containsKey("query")) {
				try {
					List<QueryConfigurationContainer> parsedQueryConfigurations = parseQueryConfiguration(configurationObj.get("query"));
					for(QueryConfigurationContainer it : parsedQueryConfigurations) {

						String statementId;
						if (it.extra != null && it.extra.containsKey("name")) {
							statementId = (String)it.extra.get("name");

							if(conf.existsStatement(statementId)) {
								Pattern pat =  Pattern.compile("^(.*)-([0-9]+)$");
								Matcher m = pat.matcher(statementId);
								if(m.matches()) {
									Integer curMax = Integer.parseInt(m.group(2));
									statementId = m.group(1) + "-" + (++curMax);
								} else {
									statementId += "-1";
								}
								it.extra.put("name", statementId);
							}
							
							conf.addStatement(statementId, it.query, null);
						} else {
							statementId = conf.addStatement(it.query);
						}
						if(it.extra != null) {
							if(it.extra.containsKey("output")) {

								List<Map<String, Object>> outputDefinitions = normalizeOutputConfigurations(it.extra.get("output"));
								for (Map<String, Object> outDefinition : outputDefinitions) {
									outDefinition.put("query", statementId);
								}
								List<OutputConfigurationContainer> outputContainers = parseOutputConfigurations(outputDefinitions);
								outputContainers.forEach(container -> handleOutputConfiguration(container));
								
							}
							
							if(it.extra.containsKey("input")) {
								List<String> inputIds = new ArrayList<>();
								List<Map<String, Object>> inputDefinitions = normalizeInputConfigurations(it.extra.get("input"));
								List<InputConfigurationContainer> inputConfigurationContainers = parseInputConfigurations(inputDefinitions);
								for (InputConfigurationContainer inputConfigurationContainer : inputConfigurationContainers) {
									String inputId;
									if (conf.hasInput(inputConfigurationContainer.inputConfiguration.name)) {
										inputId = inputConfigurationContainer.inputConfiguration.name;
									} else {
										inputId = conf.addInput(inputConfigurationContainer.inputConfiguration.createInputInstance());
									}
									inputIds.add(inputId);
									if (inputConfigurationContainer.extra.containsKey("pattern")) {
										PatternConfigurationContainer pcc = parsePatternConfiguration(inputConfigurationContainer.extra.get("pattern"));
										conf.addInputPatternReference(inputId, pcc.patternReferences);
										conf.addInputPattern(inputId, pcc.patternDefinitions);
									}
									if (inputConfigurationContainer.extra.containsKey("event-types")) {
										List<String> inputEventTypesToParse = new ArrayList<>();
										if (inputConfigurationContainer.extra.get("event-types") instanceof String) {
											inputEventTypesToParse.add((String)inputConfigurationContainer.extra.get("event-types"));
										} else if (inputConfigurationContainer.extra.get("event-types") instanceof String && ((List)inputConfigurationContainer.extra.get("event-types")).size() > 0 && ((List)inputConfigurationContainer.extra.get("event-types")).get(0) instanceof String) {
											inputEventTypesToParse.addAll((List)inputConfigurationContainer.extra.get("event-types"));
										} else {
											log.error("only strings and list of strings allowed for event-types conf property");
										}
										conf.addEventTypesToParse(inputId, inputEventTypesToParse);
									}
								}
								conf.statementDeplymentToInputMapping.put(statementId, inputIds);
							}
						}
						
					};
				} catch(Exception e) {
					log.error("adding queries failed for some reason: {}", e.getMessage());
					log.debug("adding queries failed for some reason: {}", e.getMessage(), e);
				}
			}
			
			/**
			 * Reference parsing
			 */
			if(configurationObj.containsKey("reference")) {
				try {
					List<ReferenceConfigurationContainer> parsedReferenceConfigurations = parseReferenceConfiguration(configurationObj.get("reference"));

					for(ReferenceConfigurationContainer it : parsedReferenceConfigurations) {
						Reference refObj;
						/*
						 * do not enable this yet
						 * if(it.url.startsWith("jdbc:h2:")) {
							refObj = new H2Reference(it.name, it.url);
						} else 
						 */
						if(it.url.startsWith("jdbc:sqlite:")) {
							refObj = new SQLiteReference(it.name, it.url);
						} else {
							refObj = new SQLiteReference(it.name, it.url);
							//throw new Exception("unsupported driver");
						}
						//refObj = new SQLiteReference(it.name, it.url);
						
						if(it.cacheSize != null) {
							refObj.setLRUCacheSize(it.cacheSize);
						}
						if(it.cacheMaxAge != null) {
							refObj.setCacheExpiryMaxAge(it.cacheMaxAge);
						}
						if(it.cachePurgeInterval != null) {
							refObj.setCacheExpiryPurgeInterval(it.cachePurgeInterval);
						}
						if(it.fields != null) {
							refObj.setFields(it.fields);
						}
						if(it.dropOnInit != null) {
							refObj.setDropOnInit(it.dropOnInit);
						}
						if(it.extra.containsKey("input")) {
							List<Map<String, Object>> inputDefinitions = normalizeInputConfigurations(it.extra.get("input"));
							List<InputConfigurationContainer> icc = parseInputConfigurations(inputDefinitions);
							for (InputConfigurationContainer inputConfigurationContainer : icc) {
								Input input;
								if (conf.hasInput(inputConfigurationContainer.inputConfiguration.name)) {
									input = conf.getInput(inputConfigurationContainer.inputConfiguration.name);
								} else {
									input = inputConfigurationContainer.inputConfiguration.createInputInstance();
								}
								if (ReferenceInput.class.isAssignableFrom(input.getClass())) {
									String inputId = input.id;
									if (inputConfigurationContainer.extra.containsKey("pattern")) {
										PatternConfigurationContainer pcc = parsePatternConfiguration(inputConfigurationContainer.extra.get("pattern"));
										conf.addInputPatternReference(inputId, pcc.patternReferences);
										conf.addInputPattern(inputId, pcc.patternDefinitions);
									}
									((ReferenceInput) input).setInputEventReceiver((InputEventReceiver) refObj);
									((ReferenceInput) input).setReloadCallback((ReferenceReloadCallback) refObj);
									refObj.addInput((ReferenceInput) input);
									continue;
								}
								log.warn("{} is not supported as input for reference", input.getClass().getCanonicalName());
							}
						}
						conf.lookupReferences.put(it.name, refObj);
					};
				} catch(Exception e) {
					log.error("adding references failed for some reason: {}", e.getMessage());
					log.debug("adding references failed for some reason: {}", e.getMessage(), e);
				}
			}

		}
		
		return conf;
	}
	
	/** PATTERN configuration parsing **/
	
	static class PatternConfigurationContainer {
		List<String> patternReferences = new ArrayList<String>();
		List<Map<String, Object>> patternDefinitions = new ArrayList<Map<String, Object>>();
	}
	
	private static PatternConfigurationContainer parsePatternConfiguration(Object patternDefinition) {
		PatternConfigurationContainer pcc = new PatternConfigurationContainer();

		if(patternDefinition instanceof String) {
			if(Files.exists(Paths.get((conf.sourceConfigurationPath + File.separator + (String)patternDefinition)))) {
				pcc.patternReferences.add(conf.sourceConfigurationPath + File.separator + (String)patternDefinition);
			} else {
				pcc.patternDefinitions.add(MonitorRuntimeConfiguration.createPatternFromExpression((String)patternDefinition));
			}
			
		} else if (patternDefinition instanceof List && ((List)patternDefinition).size() > 0 && ((List)patternDefinition).get(0) instanceof String) {
			for(String pat : (List<String>)patternDefinition) {
				if(Files.exists(Paths.get((conf.sourceConfigurationPath + File.separator + pat)))) {
					pcc.patternReferences.add(conf.sourceConfigurationPath + File.separator + pat);
				} else {
					pcc.patternDefinitions.add(MonitorRuntimeConfiguration.createPatternFromExpression((String)pat));
				}
			}
		} else if (patternDefinition instanceof Map) {
			pcc.patternDefinitions.add((Map<String, Object>)patternDefinition);
		} else if (patternDefinition instanceof List && ((List)patternDefinition).size() > 0 && ((List)patternDefinition).get(0) instanceof Map) {
			pcc.patternDefinitions.addAll((List<Map<String, Object>>)patternDefinition);
		}
		return pcc;
	}


	/** REFERENCE configuration parsing **/

	
	static class ReferenceConfigurationContainer {
		String name;
		String url;
		Integer cacheSize;
		Integer cacheMaxAge;
		Integer cachePurgeInterval;
		Boolean dropOnInit;
		
		Map<String, Map<String, String>> fields;
		
		Map<String, Object> extra = new HashMap<String, Object>();
		
		ReferenceConfigurationContainer(String name, String url) {
			this.name = name;
			this.url = url;
		}
		ReferenceConfigurationContainer(String name, String url, Map<String, Object> extra) {
			this.name = name;
			this.url = url;
			this.extra = extra;
		}
	}
	
	private static List<ReferenceConfigurationContainer> parseReferenceConfiguration(Object referenceDefinition) throws Exception {
		List<ReferenceConfigurationContainer> references = new ArrayList<ReferenceConfigurationContainer>();
		if(referenceDefinition instanceof String) {
			String referenceDefinitionAsString = (String)referenceDefinition;
			String[] tokens = referenceDefinitionAsString.split("/");
			String name = (tokens[tokens.length-1]).replaceAll("\\.db$", "");
			references.add(new ReferenceConfigurationContainer(name, referenceDefinitionAsString));
		} else if(referenceDefinition instanceof Map) {
			Map<String, Object> referenceDefinitionMap = (Map<String, Object>)referenceDefinition;
			if(!referenceDefinitionMap.containsKey("name") || !referenceDefinitionMap.containsKey("url")) {
				throw new Exception("name and url must be set for reference configuration");
			}
			ReferenceConfigurationContainer container = new ReferenceConfigurationContainer((String)referenceDefinitionMap.get("name"), (String)referenceDefinitionMap.get("url"));
			
			if(referenceDefinitionMap.containsKey("cache-size")) {
				container.cacheSize = (int)referenceDefinitionMap.get("cache-size");
			}
			
			if(referenceDefinitionMap.containsKey("cache-max-age")) {
				if(referenceDefinitionMap.containsKey("cache-purge-interval")) {
					container.cachePurgeInterval = (int)referenceDefinitionMap.get("cache-purge-interval");
				} else {
					throw new Exception("cache-max-age must be configured with cache-purge-interval for reference");
				}
				container.cacheMaxAge = (int)referenceDefinitionMap.get("cache-max-age");
			}
			
			if(referenceDefinitionMap.containsKey("drop-on-init") && referenceDefinitionMap.get("drop-on-init") instanceof Boolean) {
				container.dropOnInit = (Boolean)referenceDefinitionMap.get("drop-on-init");
			}

			if(referenceDefinitionMap.containsKey("input")) {
				container.extra.put("input", referenceDefinitionMap.get("input"));
			}
			
			if(referenceDefinitionMap.containsKey("fields") && referenceDefinitionMap.get("fields") instanceof Map) {
				container.fields = (Map<String, Map<String, String>>)referenceDefinitionMap.get("fields");
			}
			
			references.add(container);
		}
		
		return references;
	}


	////////////////////////////////////////
	/// 		QUERY PARSING		////////
	////////////////////////////////////////
	
	static class QueryConfigurationContainer {
		String query;
		Map<String, Object> extra;
		QueryConfigurationContainer(String query) {
			this.query = query;
		}
		QueryConfigurationContainer(String query, Map<String, Object> extra) {
			this.query = query;
			this.extra = extra;
		}
	}

	private static boolean isMonitorFile(String fileName) {
		File queryFile = new File(conf.sourceConfigurationPath + File.separator + fileName);
		return queryFile.exists();
	}
	
	private static String toMonitorFile(String fileName) {
		File queryFile = new File(conf.sourceConfigurationPath + File.separator + fileName);
		if(queryFile.exists()) {
			return queryFile.getAbsolutePath();
		} else {
			return fileName;
		}		
	}
	
	private static List<String> getGlobbedFiles(String glob) {
		if(glob.startsWith("file:")) {
			glob = glob.substring(5);
		}
		
		List<String> files = new ArrayList<String>();
		if(glob.startsWith("/")) {
			files.add(glob);
			return files;
		}
		Path dir = Path.of(conf.sourceConfigurationPath);
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
			stream.forEach( (Path file) -> {
				files.add(file.toFile().getAbsolutePath());
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return files;
	}
			
	/** Query configuration parsing **/

	private static List<QueryConfigurationContainer> handleQueryDefinitionAsString(String queryDefinition) {
		return handleQueryDefinitionAsString(queryDefinition, null);
	}
	
	private static List<QueryConfigurationContainer> handleQueryDefinitionAsString(String queryDefinition, Map<String, Object> extra) {
		
		List<QueryConfigurationContainer> containers = new ArrayList<QueryConfigurationContainer>();
		
		try {
			// at first try to convert input to uri
			URI inputURI = convertStringInputDefinitionToURI(queryDefinition, "epl");
			// now we check if file scheme is used
			if(inputURI.getScheme().equals("file")) {
				String srcFilePath = FileSystem.getPathFromURI(inputURI);
				for(String filePath : getGlobbedFiles(srcFilePath)) {
					containers.add(new QueryConfigurationContainer(filePath, extra));
				}
			
			// then we check if input is reference to a specific file 
			} else if(isMonitorFile(queryDefinition)) {
				if(extra == null) {
					extra = new HashMap<String, Object>();
				}
				if(!extra.containsKey("name")) {
					extra.put("name", queryDefinition);
				}
				queryDefinition = toMonitorFile(queryDefinition);
				containers.add(new QueryConfigurationContainer(queryDefinition, extra));
				
			// for all other options, we use full input as query
			} else {
				containers.add(new QueryConfigurationContainer(queryDefinition, extra));
			}
		} catch (Exception e) {
			// fallback to use full input as query
			containers.add(new QueryConfigurationContainer(queryDefinition, extra));
		}
		return containers;
	}
	
	/**
	 * Main entry point for query parsing
	 * 
	 * @param queryDefinition
	 * @return
	 */
	private static List<QueryConfigurationContainer> parseQueryConfiguration(Object queryDefinition) {
		List<QueryConfigurationContainer> queries = new ArrayList<QueryConfigurationContainer>();

		// definition is a string
		if(queryDefinition instanceof String) {
			String queryDefinitionAsString = (String)queryDefinition;
			queries.addAll(handleQueryDefinitionAsString(queryDefinitionAsString));

		// definition is a map, that can contain element named 'query'
		} else if (queryDefinition instanceof Map) {
			Map<String, Object> queryDefinitionMap = (Map<String, Object>)queryDefinition;
			
			if(queryDefinitionMap.containsKey("query")) {

				// this 'query' element is a string
				if(queryDefinitionMap.get("query") instanceof String) {
					String queryAsString = (String)queryDefinitionMap.get("query");
					queries.addAll(handleQueryDefinitionAsString(queryAsString, queryDefinitionMap));
					

				// this 'query' element is a list of strings
				} else if (queryDefinitionMap.get("query") instanceof List) {
					for(String queryAsString : (List<String>)queryDefinitionMap.get("query")) {
						queries.addAll(handleQueryDefinitionAsString(queryAsString, queryDefinitionMap));
						
					}
				}
			}
			
		// definition is a list
		} else if (queryDefinition instanceof List && ((List)queryDefinition).size() > 0) { // list of one of two previous types
			for(Object queryDefinitionItem : (List)queryDefinition) {
				
				// list item is a string
				if(queryDefinitionItem instanceof String) {
					String queryDefinitionAsString = (String)queryDefinitionItem;
					queries.addAll(handleQueryDefinitionAsString(queryDefinitionAsString));
					
				// list item is a map that contains element 'query' (this can be of string or list of strings)
				} else if(queryDefinitionItem instanceof Map) {
					Map<String, Object> itemAsMap = (Map<String, Object>)queryDefinitionItem;
					
					if(itemAsMap.containsKey("query")) {
						if(itemAsMap.get("query") instanceof String) {
							String queryAsString = (String)itemAsMap.get("query");
							queries.addAll(handleQueryDefinitionAsString(queryAsString, itemAsMap));
							
						} else if (itemAsMap.get("query") instanceof List) {
							for(String queryAsString : (List<String>)itemAsMap.get("query")) {
								queries.addAll(handleQueryDefinitionAsString(queryAsString, itemAsMap));
								
							}
						}
					}
				}
			}
		}
		return queries;
	}


	////////////////////////////////////////
	/// 		INPUT PARSING		////////
	////////////////////////////////////////

	static class InputConfigurationContainer {
		InputConfiguration inputConfiguration;
		Map<String, Object> extra;

		InputConfigurationContainer(InputConfiguration inputConfiguration, Map<String, Object> extra) {
			this.inputConfiguration = inputConfiguration;
			this.extra = extra;
		}
	}
	
	private static URI parseValidUri(String uriDefinition) {
		
		URI uri;
		try {
			uri = new URI(uriDefinition);
			if(uri.getScheme() != null && uri.getScheme().length() > 0 ) {
				return uri;
			} else {
				return null;
			}
		} catch (URISyntaxException e) {
			//log.debug("could not parse uri from '{}'", uriDefinition);
			return null;
		}
	}

	private static URI convertStringInputDefinitionToURI(String inputDefinition) throws Exception {
		return convertStringInputDefinitionToURI(inputDefinition, "default");
	}
	
	private static URI convertStringInputDefinitionToURI(String inputDefinition, String defaultScheme) throws Exception {
		
		URI inputURI = parseValidUri(inputDefinition);
		
		if(inputURI == null) {
			if(inputDefinition.equals("stdin")) {
				try {
					inputURI = new URI("stdin:stdin");
				} catch (URISyntaxException e) {
					throw new Exception("could not manually create stdin URI for definition " + inputDefinition);
				}
			} else {
				try {
					inputURI = new URI(defaultScheme, inputDefinition, "");
				} catch (URISyntaxException e) {
					throw new Exception("could not manually create default URI for definition" + inputDefinition);
				}
			}
		}
		
		return inputURI;
	}
	
	private static <T extends InputConfiguration<T>> InputConfiguration<T> parseInputStringDefinition(String inputDefinition) throws Exception {

		InputConfiguration<T> inputConf;
		
		URI inputURI;
		try {
			inputURI = convertStringInputDefinitionToURI(inputDefinition, "file");
		} catch(Exception e) {
			// rethrow for now
			throw e;
		}
	
		try {
			inputConf = parseInputURIDefinition(inputURI);
		} catch (Exception e) {
			log.debug("parsing input as URI failed with message: {}", e.getMessage(), e);
			throw new Exception(String.format("parsing input as URI failed with message: %s", e.getMessage()));
		}
		
		return inputConf;
	}
	
	private static <T extends InputConfiguration<T>, S extends URICapableInputConfiguration> InputConfiguration<T> parseInputURIDefinition(URI inputDefinition) throws Exception {
		
		Map<String, Class<S>> schemes = new HashMap<String, Class<S>>();
		//Map<String, URICapableInput> schemes = new HashMap<String, URICapableInput>();
		//Class<T>
		InputConfiguration.getInputConfigurationClasses().forEach( (Class confClass) -> {
			if(URICapableInputConfiguration.class.isAssignableFrom(confClass)) {
				try {
					URICapableInputConfiguration obj = (URICapableInputConfiguration)confClass.getConstructor(MonitorRuntimeConfiguration.class).newInstance(conf);
					obj.getSupportedSchemeList().forEach( (String scheme) -> {
						schemes.put(scheme, (Class<S>)confClass);
					});
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
					log.error("err : {}", e.getMessage(), e);
				}
			}
		});
		
		InputConfiguration<T> inputConf;
		if (inputDefinition != null && schemes.containsKey(inputDefinition.getScheme())) {
			Class<S> uriConfClass = schemes.get(inputDefinition.getScheme());
			inputConf = (InputConfiguration<T>)(uriConfClass.getConstructor(MonitorRuntimeConfiguration.class).newInstance(conf));
			((S)inputConf).parseURIInputDefinition(inputDefinition);
			//inputConf = schemes.get(inputDefinition.getScheme()).parseURIInputDefinition(inputDefinition);
		} else {
			throw new Exception("unknown input scheme: " + inputDefinition.getScheme());
		}
		return inputConf;
	}

	/***
	 * parse normalized input definition by creating inputconf object depending on input type and then parsing calling the config parse on it
	 *
	 * @param inputDefinition
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	private static <T extends InputConfiguration<T>> InputConfiguration<T> parseInputMapDefinition(Map<String, Object> inputDefinition) throws Exception {
		
		Map<String, Class<T>> inputTypes = new HashMap<String, Class<T>>();

		InputConfiguration.getInputConfigurationClasses().forEach( (Class confClass) -> {
			
			try {
				InputConfiguration<T> obj = (InputConfiguration<T>)confClass.getConstructor(MonitorRuntimeConfiguration.class).newInstance(conf);
				inputTypes.put(obj.getInputType(), confClass);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				log.error("err : {}", e.getMessage(), e);
			}
		});
		
		InputConfiguration<T> inputConf;
		if (inputDefinition != null && inputDefinition.containsKey("type") && inputTypes.containsKey(inputDefinition.get("type"))) {
			Class<T> inputConfClass = inputTypes.get(inputDefinition.get("type"));
			inputConf = (InputConfiguration<T>)(inputConfClass.getConstructor(MonitorRuntimeConfiguration.class).newInstance(conf));
			inputConf.parseMapInputDefinition(inputDefinition);
		} else {
			throw new Exception("unknown input type: " + inputDefinition.get("type"));
		}
		return inputConf;
	}

	/***
	 * parse normalized input configurations to list of InputConfigurationContainers
	 *
	 * @param inputDefinitions
	 * @param <T>
	 * @return
	 */
	private <T extends InputConfiguration<T>> List<InputConfigurationContainer> parseInputConfigurations(List<Map<String, Object>> inputDefinitions) {
		List<InputConfigurationContainer> inputConfigurationContainers = new ArrayList<InputConfigurationContainer>();
		for(Map<String, Object> inputDefinition : inputDefinitions) {
			try {
				InputConfiguration<T> inputConfiguration = parseInputMapDefinition(inputDefinition);
				inputConfigurationContainers.add(new InputConfigurationContainer(inputConfiguration, inputDefinition));
			} catch(Exception e) {
				log.error("shit failed", e);
			}
		}
		return inputConfigurationContainers;
	}

	/**
	 * Initialize and register input from inputconfigurationcontainer
	 * 
	 * @param inputConfigurationContainer
	 * @return
	 */
	private void handleInputConfiguration(InputConfigurationContainer inputConfigurationContainer) {
		try {
			String inputId;

			// create and register input if not done yet
			if (conf.hasInput(inputConfigurationContainer.inputConfiguration.name)) {
				log.debug("input already registered with id {}", inputConfigurationContainer.inputConfiguration.name);
				inputId = inputConfigurationContainer.inputConfiguration.name;
			} else {
				Input input = inputConfigurationContainer.inputConfiguration.createInputInstance();
				inputId = conf.addInput(input);
				log.debug("registering input {}", inputId);
			}

			// handle input specific pattern
			if (inputConfigurationContainer.extra.containsKey("pattern")) {
				PatternConfigurationContainer pcc = parsePatternConfiguration(inputConfigurationContainer.extra.get("pattern"));
				conf.addInputPatternReference(inputId, pcc.patternReferences);
				conf.addInputPattern(inputId, pcc.patternDefinitions);
			}

			// handle input-specifi event-types
			if (inputConfigurationContainer.extra.containsKey("event-types")) {
				List<String> inputEventTypesToParse = new ArrayList<>();
				if (inputConfigurationContainer.extra.get("event-types") instanceof String) {
					inputEventTypesToParse.add((String)inputConfigurationContainer.extra.get("event-types"));
				} else if (inputConfigurationContainer.extra.get("event-types") instanceof String && ((List)inputConfigurationContainer.extra.get("event-types")).size() > 0 && ((List)inputConfigurationContainer.extra.get("event-types")).get(0) instanceof String) {
					inputEventTypesToParse.addAll((List)inputConfigurationContainer.extra.get("event-types"));
				} else {
					log.error("only strings and list of strings allowed for event-types conf property");
				}
				conf.addEventTypesToParse(inputId, inputEventTypesToParse);
			}

			// handle queries
			if (inputConfigurationContainer.extra.containsKey("query")) {
				List<QueryConfigurationContainer> parsedQueryConfigurations = parseQueryConfiguration(inputConfigurationContainer.extra.get("query"));
				for (QueryConfigurationContainer innerInput : parsedQueryConfigurations) {
					String statementId;
					if (innerInput.extra != null && innerInput.extra.containsKey("name")) {
						statementId = (String)innerInput.extra.get("name");
						conf.addStatement(statementId, innerInput.query, null);
					} else {
						statementId = conf.addStatement(innerInput.query);
					}
					log.debug("statement: {} -> input: {}", statementId, Arrays.asList(new String[] { inputId }));
					conf.statementDeplymentToInputMapping.put(statementId, Arrays.asList(new String[] { inputId }));
					if (innerInput.extra.containsKey("output")) {
						List<Map<String, Object>> outputDefinitions = normalizeOutputConfigurations(innerInput.extra.get("output"));
						outputDefinitions.forEach(def -> def.put("query", statementId));
						List<OutputConfigurationContainer> outConfigurations = parseOutputConfigurations(outputDefinitions);
						outConfigurations.forEach(outConf -> handleOutputConfiguration(outConf));
					}
				}
			}
		} catch (Exception e) {
			log.error("Could not parse/register input. Cause: {}", e.getMessage());
			log.debug("Could not parse/register input. Cause: {}", e.getMessage(), e);
		}
	}


	/***
	 * Generate list of input hashmaps from yaml configuration
	 *
	 * @param inputDefinition
	 * @param <T>
	 * @param <S>
	 * @return
	 */
	private <T extends InputConfiguration<T>, S extends URICapableInputConfiguration> List<Map<String, Object>> normalizeInputConfigurations(Object inputDefinition) {

		Map<String, Class<S>> schemes = new HashMap<String, Class<S>>();
		InputConfiguration.getInputConfigurationClasses().forEach( (Class confClass) -> {
			if(URICapableInputConfiguration.class.isAssignableFrom(confClass)) {
				try {
					URICapableInputConfiguration obj = (URICapableInputConfiguration)confClass.getConstructor(MonitorRuntimeConfiguration.class).newInstance(conf);
					obj.getSupportedSchemeList().forEach( (String scheme) -> {
						schemes.put(scheme, (Class<S>)confClass);
					});
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
					log.error("err : {}", e.getMessage(), e);
				}
			}
		});

		List<Map<String, Object>> inputDefinitionList = new ArrayList<>();
		if (inputDefinition instanceof String) {
			try {
				URI inputDefAsURI = convertStringInputDefinitionToURI((String)inputDefinition);
				if (inputDefAsURI != null && schemes.containsKey(inputDefAsURI.getScheme())) {
					Class<S> uriConfClass = schemes.get(inputDefAsURI.getScheme());
					InputConfiguration<T> inputConf = (InputConfiguration<T>)(uriConfClass.getConstructor(MonitorRuntimeConfiguration.class).newInstance(conf));
					Map<String, Object> parsedConf = ((URICapableInputConfiguration)inputConf).parseURIInputDefinition(inputDefAsURI);

					// check if named input, and if already parsed
					if(parsedConf.containsKey("name") && namedInputs.containsKey((String)parsedConf.get("name"))) {
						log.warn("input {} already registered, skipping", parsedConf.get("name"));
					} else {
						if(parsedConf.containsKey("name")) {
							namedInputs.put((String)parsedConf.get("name"), (String)parsedConf.get("type"));
						}
						inputDefinitionList.add(parsedConf);
					}

				} else {
					throw new Exception("unknown input scheme: " + inputDefAsURI.getScheme());
				}
			} catch (Exception exception) {}
		} else if (inputDefinition instanceof List && ((List)inputDefinition).size() > 0 && ((List)inputDefinition).get(0) instanceof String) {
			for (String inputDefinitionAsString : (List<String>)inputDefinition) {
				try {
					URI inputDefAsURI = convertStringInputDefinitionToURI(inputDefinitionAsString);
					if (inputDefAsURI != null && schemes.containsKey(inputDefAsURI.getScheme())) {
						Class<S> uriConfClass = schemes.get(inputDefAsURI.getScheme());
						InputConfiguration<T> inputConf = (InputConfiguration<T>)(uriConfClass.getConstructor(MonitorRuntimeConfiguration.class).newInstance(conf));
						Map<String, Object> parsedConf = ((URICapableInputConfiguration)inputConf).parseURIInputDefinition(inputDefAsURI);

						if(parsedConf.containsKey("name") && namedInputs.containsKey((String)parsedConf.get("name"))) {
							log.warn("input {} already registered, skipping", parsedConf.get("name"));
						} else {
							if(parsedConf.containsKey("name")) {
								namedInputs.put((String)parsedConf.get("name"), (String)parsedConf.get("type"));
							}
							inputDefinitionList.add(parsedConf);
						}
					}
					throw new Exception("unknown input scheme: " + inputDefAsURI.getScheme());
				} catch (Exception exception) {}
			}
		} else if (inputDefinition instanceof Map) {
			Map<String, Object> parsedConf = (Map<String, Object>)inputDefinition;
			if(parsedConf.containsKey("name") && namedInputs.containsKey((String)parsedConf.get("name"))) {
				log.warn("input {} already registered, skipping", parsedConf.get("name"));
			} else {
				if(parsedConf.containsKey("name")) {
					namedInputs.put((String)parsedConf.get("name"), (String)parsedConf.get("type"));
				}
				inputDefinitionList.add(parsedConf);
			}
		} else if (inputDefinition instanceof List && ((List)inputDefinition).size() > 0 && ((List)inputDefinition).get(0) instanceof Map) {
			for(Map<String, Object> parsedConf : (List<Map<String, Object>>)inputDefinition) {
				if(parsedConf.containsKey("name") && namedInputs.containsKey((String)parsedConf.get("name"))) {
					log.warn("input {} already registered, skipping", parsedConf.get("name"));
				} else {
					if(parsedConf.containsKey("name")) {
						namedInputs.put((String)parsedConf.get("name"), (String)parsedConf.get("type"));
					}
					inputDefinitionList.add(parsedConf);
				}
			}
			//inputDefinitionList.addAll((List)inputDefinition);
		}

		return inputDefinitionList;
	}
	
	
	////////////////////////////////////////
	/// 		OUTPUT PARSING		////////
	////////////////////////////////////////
	
	static class OutputConfigurationContainer {
		OutputConfiguration outputConfiguration;
		Map<String, Object> extra;
		OutputConfigurationContainer(OutputConfiguration outputConfiguration, Map<String, Object> extra) {
			this.outputConfiguration = outputConfiguration;
			this.extra = extra;
		}
	}
	
	private static List<OutputConfigurationContainer> parseOutputConfiguration(Object outputDefinition) {
		List<Map<String, Object>> outputDefinitionList = new ArrayList<Map<String, Object>>();
		// normalize all allowed output configurations to list of maps and then iterate
		
		// we have a string
		if(outputDefinition instanceof String) { // we have string as output def, so we use it as FileOutput (and value as file name) || special cases - if output def == "stdout" then we create stdoutOutput, "noop" = NoopOutput
			if(((String)outputDefinition).equals("stdout")) {
				outputDefinitionList.add(new HashMap<String, Object>() {{
					put("type", "stdout");
				}});
			} else if(((String)outputDefinition).equals("noop")) { // short for noop type output
				outputDefinitionList.add(new HashMap<String, Object>() {{
					put("type", "noop");
				}});
			} else if(((String)outputDefinition).equals("seq")) { // short for sequence counter output using stdout (useful for testing)
				outputDefinitionList.add(new HashMap<String, Object>() {{
					put("type", "seq");
					put("output", "stdout");
				}});
			} else {
				Map<String, Object> tmpFileConf = new HashMap<String, Object>();
				tmpFileConf.put("type", "file");
				tmpFileConf.put("file", (String)outputDefinition);
				outputDefinitionList.add(tmpFileConf);
			}
		}

		// we have list of strings - actually we don't have a valid case for this yet
		if(outputDefinition instanceof List && ((List)outputDefinition).size() > 0 && ((List)outputDefinition).get(0) instanceof String) {
			throw new UnsupportedOperationException("list of strings for output configuration is not implemented yet");
		}
		
		// we have a map
		if(outputDefinition instanceof Map) {
			outputDefinitionList.add((Map<String, Object>)outputDefinition);
		}
		
		// we have lits of maps
		if(outputDefinition instanceof List && ((List)outputDefinition).size() > 0 && ((List)outputDefinition).get(0) instanceof Map) {
			outputDefinitionList.addAll((List<Map<String, Object>>)outputDefinition);
		}
		
		return parseOutputConfigurations(outputDefinitionList);
	}


	/***
	 * parse normalized output definition by creating outputconf object depending on output type
	 *
	 * @param outputDefinition
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	private static <T extends OutputConfiguration<T>> OutputConfiguration<T> parseOutputMapDefinition(Map<String, Object> outputDefinition) throws Exception {
		
		Map<String, Class<T>> outputTypes = new HashMap<String, Class<T>>();

		OutputConfiguration.getOutputConfigurationClasses().forEach( (Class confClass) -> {
			
			try {
				OutputConfiguration<T> obj = (OutputConfiguration<T>)confClass.getConstructor(MonitorRuntimeConfiguration.class).newInstance(conf);
				outputTypes.put(obj.getOutputType(), confClass);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				log.error("err : {}", e.getMessage(), e);
			}
		});
		
		OutputConfiguration<T> outputConf;
		if (outputDefinition != null && outputDefinition.containsKey("type") && outputTypes.containsKey(outputDefinition.get("type"))) {
			Class<T> outputConfClass = outputTypes.get(outputDefinition.get("type"));
			outputConf = (OutputConfiguration<T>)(outputConfClass.getConstructor(MonitorRuntimeConfiguration.class).newInstance(conf));
			outputConf.parseMapOutputDefinition(outputDefinition);
		} else {
			throw new Exception("unknown input type: " + outputDefinition.get("type"));
		}
		return outputConf;
	}


	/***
	 * parse normalized output configurations to list of OutputConfigurationContainers
	 *
	 * @param inputDefinitions
	 * @param <T>
	 * @return
	 */
	private static <T extends OutputConfiguration<T>> List<OutputConfigurationContainer> parseOutputConfigurations(List<Map<String, Object>> outputDefinitions) {
		List<OutputConfigurationContainer> outputConfigurationContainers = new ArrayList<OutputConfigurationContainer>();
		for(Map<String, Object> outputDefinition : outputDefinitions) {
			try {
				OutputConfiguration<T> outputConfiguration = parseOutputMapDefinition(outputDefinition);
				outputConfigurationContainers.add(new OutputConfigurationContainer(outputConfiguration, outputDefinition));
			} catch(Exception e) {
				log.error("output conf parsing failed", e);
			}
		}
		return outputConfigurationContainers;
	}

	/**
	 * Initialize and register output from OutputConfigurationContainer
	 * 
	 * @param outputConfigurationContainer
	 * @return
	 */
	private void handleOutputConfiguration(OutputConfigurationContainer outConfContainer) {
		String outputId = UUID.randomUUID().toString();
		if (outConfContainer.extra.containsKey("name") && ((String)outConfContainer.extra.get("name")).length() > 0)
			outputId = (String)outConfContainer.extra.get("name");
		if (conf.getListener(outputId) == null) {
			Output output = outConfContainer.outputConfiguration.createOutputInstance();
			try {
				conf.addListener(outputId, (CustomUpdateListener)new EventUpdateListener(output));
			} catch (Exception e) {
				log.debug("Could not register ouput with type {} (id: {}). Cause: {}", output.getClass().getSimpleName(), outputId, e.getMessage(), e);
			}
		} else {
			log.debug("listener with id {} already exists, skipping this one .. ", outputId);
		}

		// add redirection
		if (outConfContainer.extra.containsKey("query")) {
			if (!conf.outputToStatementRedirection.containsKey(outputId))
				conf.outputToStatementRedirection.put(outputId, new ArrayList());
			if (outConfContainer.extra.get("query") instanceof String) {
				((List<String>)conf.outputToStatementRedirection.get(outputId)).add((String)outConfContainer.extra.get("query"));
			} else if (outConfContainer.extra.get("query") instanceof List) {
				((List)conf.outputToStatementRedirection.get(outputId)).addAll((List)outConfContainer.extra.get("query"));
			}
		}
	}



	/***
	 * Generate list of output hashmaps from yaml configuration
	 *
	 * @param outputDefinition
	 * @param <T>
	 * @param <S>
	 * @return
	 */
	private List<Map<String, Object>> normalizeOutputConfigurations(Object outputDefinition) {
		List<Map<String, Object>> outputDefinitionList = new ArrayList<>();
		if (outputDefinition instanceof String)
			if (((String)outputDefinition).equals("stdout")) {
				outputDefinitionList.add(new HashMap<String, Object>() {{
					put("type", "stdout");
				}});
			} else if (((String)outputDefinition).equals("noop")) {
				outputDefinitionList.add(new HashMap<String, Object>() {{
					put("type", "noop");
				}});
			} else if (((String)outputDefinition).equals("seq")) {
				outputDefinitionList.add(new HashMap<String, Object>() {{
					put("type", "seq");
					put("output", "stdout");
				}});
			} else if (conf.getListener((String)outputDefinition) != null) {
				// We have existing output (already defined & registered)
				Map<String, Object> tmpFileConf = new HashMap<String, Object>();
				tmpFileConf.put("name", (String)outputDefinition);
				outputDefinitionList.add(tmpFileConf);
			} else {
				Map<String, Object> tmpFileConf = new HashMap<String, Object>();
				tmpFileConf.put("type", "file");
				tmpFileConf.put("file", (String)outputDefinition);
				outputDefinitionList.add(tmpFileConf);
			}

		// we have list of strings - actually we don't have a valid case for this yet
		if(outputDefinition instanceof List && ((List)outputDefinition).size() > 0 && ((List)outputDefinition).get(0) instanceof String) {
			throw new UnsupportedOperationException("list of strings for output configuration is not implemented yet");
		}

		// we have a map
		if(outputDefinition instanceof Map) {
			Map<String, Object> parsedConf = (Map<String, Object>)outputDefinition;
			outputDefinitionList.add(parsedConf);
			/*if(parsedConf.containsKey("name") && namedOutputs.containsKey((String)parsedConf.get("name"))) {
				log.warn("output {} already registered, skipping", parsedConf.get("name"));
			} else {
				if(parsedConf.containsKey("name")) {
					namedOutputs.put((String)parsedConf.get("name"), (String)parsedConf.get("type"));
				}
				outputDefinitionList.add(parsedConf);
			}*/
		}

		// we have lits of maps
		if(outputDefinition instanceof List && ((List)outputDefinition).size() > 0 && ((List)outputDefinition).get(0) instanceof Map) {
			for(Map<String, Object> parsedConf : (List<Map<String, Object>>)outputDefinition) {
				outputDefinitionList.add(parsedConf);
				/*if(parsedConf.containsKey("name") && namedOutputs.containsKey((String)parsedConf.get("name"))) {
					log.warn("output {} already registered, skipping", parsedConf.get("name"));
				} else {
					if(parsedConf.containsKey("name")) {
						namedOutputs.put((String)parsedConf.get("name"), (String)parsedConf.get("type"));
					}
					outputDefinitionList.add(parsedConf);
				}*/
			}
		}

		return outputDefinitionList;
	}
}
