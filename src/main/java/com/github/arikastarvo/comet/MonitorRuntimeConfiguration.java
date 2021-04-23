package com.github.arikastarvo.comet;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.persistence.PersistenceConfiguration;
import com.github.arikastarvo.comet.reference.Reference;

public class MonitorRuntimeConfiguration {

	Logger log = LogManager.getLogger(MonitorRuntimeConfiguration.class);

	/**
	 * data path for pesistence 
	 */
	
	public static final String RUNTIME_NAME_UNSET = "__UNSET"; 
	
	public static final String DEFAULT_PATTERNS = "__DEFAULT_PATTERNS__"; 
	
	public static final String DEFAULT_EVENT_TYPE = "events";
	
	public CometApplicationConfiguration applicationConfiguration;
	
	public String runtimeName = RUNTIME_NAME_UNSET;

	public String dataPath = null;
	
	public String sourceConfigurationPath;
	
	/***
	 * If this is 0 or greater, this is the starting time for esper runtime. For some specific use-cases (mainly for testing) it's needed to set this explicitly. 
	 * Represented in milliseconds from epoch
	 */
	public long initialTime = -1L;
	
	/***
	 *	null equals command line args (as only one monitor can be started like this), other values are paths to configuration files
	 * 
	 */
	public String configurationSource = null;
	
	public Boolean keepMatches = false;
	public Boolean removeRawData = false;
	
	private List<String> patterns_OLD = new ArrayList<String>();
	private Map<String, List<String>> inputPatterns_OLD = new HashMap<String, List<String>>();

	
	
	/*** PATTERN REFERENCES (paths to files with pattern definitions) ***/
	
	/** 
	 * This holds references to pattern definitions files OR just raw pattern regex itself (without metainfo)
	 * 
	 */
	private Map<String, List<String>> patternReferences = new HashMap<String, List<String>>();

	
	/**
	 * Add new pattern reference (file path) to default list
	 * 
	 * @param newPatternReference Absolute (or relative to something? TODO) file path of pattern definition file
	 */
	public void addPatternReference(String newPatternReference) {
		addPatternReference(Arrays.asList(newPatternReference));
	}
	
	/**
	 * Add list of pattern references (file paths) to default list
	 * 
	 * @param newPatternReferences
	 */
	public void addPatternReference(List<String> newPatternReferences) {
		addPatternReference(newPatternReferences, null);
	}

	public void addInputPatternReference(String inputId, String newPatternReference) {
		addPatternReference(Arrays.asList(newPatternReference), inputId);
	}
	
	public void addInputPatternReference(String inputId, List<String> newPatternReferences) {
		addPatternReference(newPatternReferences, inputId);
	}

	public List<String> getDefaultPatternReferences() {
		if(patternReferences.containsKey(DEFAULT_PATTERNS)) {
			return patternReferences.get(DEFAULT_PATTERNS);
		} else {
			return new ArrayList<String>();
		}
	}

	public List<String> getPatternReferences(String inputId) {
		if(patternReferences.containsKey(inputId)) {
			return patternReferences.get(inputId);
		} else {
			return new ArrayList<String>();
		}
	}

	public Map<String, List<String>> getPatternReferences() {
		return patternReferences;
	}
	
	/**
	 * ALL pattern reference adding goes trough this method
	 * 
	 * @param newPatternReferences
	 * @param inputId
	 */
	private void addPatternReference(List<String> newPatternReferences, String inputId) {
		if(inputId == null) {
			inputId = DEFAULT_PATTERNS;
		}
		if(!patternReferences.containsKey(inputId)) {
			patternReferences.put(inputId, new ArrayList<String>());
		}
		patternReferences.get(inputId).addAll(newPatternReferences);
	}
	
	/*** PATTERN DEFINITIONS ***/
	
	/**
	 * This holds real pattern configurations
	 * 
	 */
	private Map<String, List<Map<String, Object>>> patterns = new HashMap<String, List<Map<String, Object>>>(); 
	

	private static Pattern pat = Pattern.compile("^(?<type>[a-zA-Z][a-zAZ0-9-_]*)@@(?<pattern>.*)$");
	/**
	 * takes in regex/grok pattern expression and returns pattern definition
	 * 
	 * @param expression
	 * @return
	 */
	public static Map<String, Object> createPatternFromExpression(String expression) {
		
		Matcher m = pat.matcher(expression);
		if(m.matches()) {
			return new HashMap<String, Object>() {{
				put("name", m.group("type"));
				put("pattern", m.group("pattern"));
			}};
		} else {
			return new HashMap<String, Object>() {{
				put("name", DEFAULT_EVENT_TYPE);
				put("pattern", expression);
			}};
		}
	}
	
	/**
	 * Add regex/grok expression to default patterns (as default eventType)
	 * 
	 * @param newExpression
	 */
	public void addPatternExpression(String newExpression) {
		addPattern(createPatternFromExpression(newExpression));
	}
	
	/**
	 * Add list of regex/grok expressions to default patterns (as default eventType)
	 * 
	 * @param newExpressions
	 */
	public void addPatternExpression(List<String> newExpressions) {
		List<Map<String, Object>> newPatterns = newExpressions.stream().map( expression -> createPatternFromExpression(expression)).collect(Collectors.toList());
		addPattern(newPatterns);
	}
	
	/**
	 * Add one pattern to default patterns
	 * 
	 * @param newPattern
	 */
	public void addPattern(Map<String, Object> newPattern) {
		addPattern(Arrays.asList(newPattern));
	}

	/**
	 * Add list of patterns to default patterns
	 * 
	 * @param newPatterns
	 */
	public void addPattern(List<Map<String, Object>> newPatterns) {
		addPattern(newPatterns, null);
	}
	
	/**
	 * Add list of patterns to for an input
	 * 
	 * @param pattern
	 */
	private void addPattern(List<Map<String, Object>> newPatterns, String inputId) {
		if(inputId == null) {
			inputId = DEFAULT_PATTERNS;
		}
		if(!patterns.containsKey(inputId)) {
			patterns.put(inputId, new ArrayList<Map<String, Object>>());
		}
		patterns.get(inputId).addAll(newPatterns);
	}
	
	/**
	 * return all registered patterns (for all inputs)
	 * 
	 * @return
	 */
	public Map<String, List<Map<String, Object>>> getPatterns() {
		return patterns;
	}

	/**
	 * return list of default patterns (no associated with any input)
	 * 
	 * @return
	 */
	public List<Map<String, Object>> getDefaultPatterns() {
		return getPatterns(DEFAULT_PATTERNS);
	}
	
	/**
	 * return list of patterns associated with a specific input
	 * 
	 * @param inputId
	 * @return
	 */
	public List<Map<String, Object>> getPatterns(String inputId) {
		if(patterns.containsKey(inputId)) {
			return patterns.get(inputId);
		} else {
			// return an empty list
			return new ArrayList<Map<String, Object>>();
		}
	}
	
	
	public void addInputPatternExpression(String inputId, String expression) {
		addPattern(Arrays.asList(createPatternFromExpression(expression)), inputId);
	}
	
	public void addInputPatternExpression(String inputId, List<String> expressions) {
		List<Map<String, Object>> newPatterns = expressions.stream().map( expression -> createPatternFromExpression(expression)).collect(Collectors.toList());
		addPattern(newPatterns, inputId);
	}

	public void addInputPattern(String inputId, List<Map<String, Object>> newPatterns) {
		addPattern(newPatterns, inputId);
	}

	public void addInputPattern(String inputId, Map<String, Object> newPattern) {
		addPattern(Arrays.asList(newPattern), inputId);
	}
	
	/** generic helpers **/
	
	public List<String> getInputsIdsWithPatterns() {
		Set<String> inputIds = new HashSet<String>(getPatternReferences().keySet());
		inputIds.addAll(getPatterns().keySet().stream().collect(Collectors.toList()));
		return inputIds.stream().filter( id -> !id.equals(DEFAULT_PATTERNS)).collect(Collectors.toList());
	}


	/** filter out event types not needed **/

	private Map<String, List<String>> eventTypesToParse = new HashMap<String, List<String>>();	

	public void addEventTypeToParse(String eventType) {
		addEventTypeToParse(null, eventType);
	}
	
	public void addEventTypeToParse(String inputId, String eventType) {
		addEventTypesToParse(inputId, Arrays.asList(eventType));
	}
	
	public void addEventTypesToParse(List<String> eventTypes) {
		addEventTypesToParse(null, eventTypes);
	}
		
	public void addEventTypesToParse(String inputId, List<String> eventTypes) {
		/*if(inputId == null) {
			inputId = DEFAULT_PATTERNS;
		}*/
		
		/**
		 * TODO: currently all event-types are forced to one list because all input's done have a separate parser this list could operate on
		 */
		inputId = DEFAULT_PATTERNS;
		
		if(!eventTypesToParse.containsKey(inputId)) {
			eventTypesToParse.put(inputId, new ArrayList<String>());
		}
		eventTypesToParse.get(inputId).addAll(eventTypes);
	}
	
	public List<String> getDefaultEventTypesToParse() {
		return getEventTypesToParse(DEFAULT_PATTERNS);
	}
	
	public List<String> getEventTypesToParse(String inputId) {
		return eventTypesToParse.get(inputId);
	}
	
	/** END PATTERN RELATED STUFF **/
	
	/**
	 * Wheather to use internal embedded patterns or not
	 */
	public Boolean usePatternset = true;
	
	// set default clock type
	public Boolean externalClock = false;
	
	// if monitor locked/pinned to given dc
	public Boolean locked = false;
	
	// persistence
	public PersistenceConfiguration persistenceConfiguration = new PersistenceConfiguration();
	
	private List<Map<String, Object>> configurationTimeEventTypes = new ArrayList<Map<String, Object>>();
	
	public void addEventType(String eventType) {
		addEventType(eventType, MonitorRuntimeConfiguration.DEFAULT_EVENT_TYPE, new HashMap<String, Object>());
	}
	
	public void addEventType(String eventType, Map<String, Object> fields) {
		addEventType(eventType, MonitorRuntimeConfiguration.DEFAULT_EVENT_TYPE, fields);
	}
	
	public void addEventType(String eventType, String parent, Map<String, Object> fields) {
		configurationTimeEventTypes.add(new HashMap<String, Object>() {{
			put("name", eventType);
			put("parent", Arrays.asList(parent));
			put("fields", fields);
		}});
	}
	
	public List<Map<String,Object>> getEventTypes() {
		return configurationTimeEventTypes;
	}
	
	/** 
	 * 
	 * INPUT 
	 * 
	 * **/
	public Map<String, List<String>> statementDeplymentToInputMapping = new HashMap<String, List<String>>();
	//private List<Input> inputs = new ArrayList<Input>();
	private Map<String, Input> inputs = new HashMap<String, Input>();

	public boolean hasInputs() {
		return inputs.size() > 0;
	}

	public boolean hasInput(String id) {
		return inputs.containsKey(id);
	}

	public Input getInput(String id) {
		return inputs.get(id);
	}
	
	public Map<String, Input> getInputs() {
		return inputs;
	}
	
	public void removeInput(String inputId) {
		if(inputs.containsKey(inputId)) {
			inputs.remove(inputId);
		}
	}
	
	public String addInput(Input input) {
		return addInput(input.id, input);
	}
	
	public String addInput(String id, Input input) {
		if(!inputs.containsKey(id)) {
			input.id = id;
			log.debug("registering input '{}' with id '{}'", input.getClass().getSimpleName(), id);
			inputs.put(id, input);
		} else {
			log.error("input with id '{}' already registered, not starting new input with same id", id);
			throw new IllegalArgumentException("duplicate input id");
		}
		return id;
	}
	
	
	/** SECRETS **/
	
	public String secretsPath;


	/*** secrets management ***/
	
	public Map<String, Map<String, Object>> getApplicationSecrets() {
		if(secretsPath != null) {
			return CometApplication.getSecrets(new File(secretsPath));
		} else {
			return CometApplication.getSecrets();
		}
	}
	
	public Map<String, Object> getApplicationSecret(String secret) {
		if(secretsPath != null) {
			return CometApplication.getSecret(new File(secretsPath), secret);
		} else {
			return CometApplication.getSecret(secret);
		}
	}
	
	/** end secrets-management **/
	
	
	
	/** 
	 * EPL queries
	 * 
	 * TODO! this statements var should be renamed to deployments for clarity !!!
	 * 
	 */
	
	private Map<String, Map<String, String>> statements = new LinkedHashMap<String, Map<String, String>>();
	
	public Map<String, Map<String, String>> getStatements() {
		return statements;
	}
	
	public boolean existsStatement(String statementId) {
		return statements.containsKey(statementId);
	}
	
	public boolean hasStatements() {
		return statements.size() > 0;
	}
	
	public String addStatement(String statement) {
		return addStatement(UUID.randomUUID().toString(), statement, null);
	}
	
	public void removeAllStatements() {
		log.debug("removing all statements");
		statements = new LinkedHashMap<String, Map<String, String>>();
	}
	
	public String addStatement(String uuid, String statement, String type) {
		// check if query is a file containing query
		File file = new File(statement);
		if(file.exists() && file.isFile()) {
			try {
				byte[] encoded = Files.readAllBytes(Paths.get(file.getAbsolutePath()));
				
				//statement.add(new String(encoded, "UTF-8"));
				statements.put(uuid, new HashMap<String, String>() {{
					put("statement", new String(encoded, "UTF-8"));
					put("type", "file");
					put("filename", file.getAbsolutePath());
				}});
			} catch (IOException e) {
				// this is normal, skip ahead
			}
		} else if(CometApplication.class.getClassLoader().getResource(statement) != null) {
			InputStream in = CometApplication.class.getClassLoader().getResourceAsStream(statement);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
	        //statement.add(reader.lines().collect(Collectors.joining(System.lineSeparator())));
	        statements.put(uuid, new HashMap<String, String>() {{
				put("statement", reader.lines().collect(Collectors.joining(System.lineSeparator())));
				put("type", "other");
			}});
		} else {
			statements.put(uuid, new HashMap<String, String>() {{
				put("statement", statement);
				put("type", (type != null?type:"other"));
			}});
		}
		return uuid;
	}

	/**
	 * extra-statements is for inputs that register some sort of statements that must be executed before regular queries
	 */
	private List<String> extraStatements = new ArrayList<String>();
	
	public void addExtraStatement(String statement) {
		this.extraStatements.add(statement);
	}
	
	public List<String> getExtraStatements() {
		return this.extraStatements;
	}
	
	
	/**
	 *  
	 * OUTPUT
	 * 
	 */
	
	/** stdout/general **/
	
	public String outputFormat = null;
	
	/** output redirection **/
	public Map<String,List<String>> outputToStatementRedirection = new HashMap<String, List<String>>();
	//public Map<String,List<String>> statementDeplymentToOutputRedirection = new HashMap<String, List<String>>();
	
	/** listeners **/
	private Map<String, CustomUpdateListener> listeners = new HashMap<String, CustomUpdateListener>();

	public String addListener(String id, CustomUpdateListener listener) {
		if(!listeners.containsKey(id)) {
			log.debug("registering output '{}' with id '{}'", listener.getOutput().getClass().getName(), id);
			listeners.put(id, listener);
		} else {
			log.error("listener with id '{}' already registered, not starting new listener with same id", id);
			throw new IllegalArgumentException("duplicate listener id");
		}
		return id;
	}
	
	public String addListener(CustomUpdateListener listener) {
		String uuid = UUID.randomUUID().toString();
		return addListener(uuid, listener);
	}
	
	public String getDefaultDataPath() {
		if(this.dataPath != null) {
			return this.dataPath;
		} else if (applicationConfiguration != null) {
			return applicationConfiguration.defaultDataPath;
		} else {
			return null;
		}
	}

	public void removeListeners() {
		log.debug("clearing all outputs");
		this.listeners.clear();
	}

	public void removeListener(String listenerId) {
		log.debug("removing output {}", listenerId);
		this.listeners.remove(listenerId);
	}
	
	public Map<String, CustomUpdateListener> getListeners() {
		return this.listeners;
	}
	
	public CustomUpdateListener getListener(String listenerId) {
		return this.listeners.get(listenerId);
	}
	
	/**
	 * 
	 * REFERENCE
	 * 
	 */

	public Map<String, Reference> lookupReferences = new HashMap<String, Reference>();
	
	/**
	 * 
	 * CONSTRUCTORS
	 * 
	 */
	public MonitorRuntimeConfiguration() {
		log = LogManager.getLogger(MonitorRuntimeConfiguration.class);
		this.applicationConfiguration = new CometApplicationConfiguration();
	}
	
	public MonitorRuntimeConfiguration(CometApplicationConfiguration applicationConfiguration) {
		log = LogManager.getLogger(MonitorRuntimeConfiguration.class);
		this.applicationConfiguration = applicationConfiguration;
	}
}
