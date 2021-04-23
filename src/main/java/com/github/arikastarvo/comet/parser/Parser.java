package com.github.arikastarvo.comet.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.CodeSource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.github.arikastarvo.comet.CustomStats;
import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.CometApplication;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.JsonException;
import com.jsoniter.spi.TypeLiteral;
import com.opencsv.CSVParser;

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.GrokUtils;
import io.krakens.grok.api.Match;
import io.krakens.grok.api.exception.GrokException;


public class Parser {

	private List<PatternDefinition> patterns;
	private CustomStats customStats = null;
	
	public static final String DEFAULT_EVENT_TYPE = "events";

	/**
	 * if this is a non-null list of strings then only these event types (and their parents) will be used for parsing 
	 */
	private List<String> eventTypesToParse = null;
	
	// preinit csv parser (even if we don't need it most of the time).. should be lazy-inited actually
	private CSVParser csvParser = new CSVParser();

	private GrokCompiler gc = GrokCompiler.newInstance();

	Logger log = LoggerFactory.getLogger(Parser.class);
	
	private List<PatternDefinition> preconfiguredEventTypes; 
	
	public Parser() throws Exception {
		this(null, null, null, false, true);
	}

	public Parser(List<String> patternReferences, List<Map<String, Object>> patternDefinitions, boolean stats, boolean useInternalPatterns) throws Exception {
		this(patternReferences, patternDefinitions, null, stats, useInternalPatterns);
	}
	
	public Parser(List<String> patternReferences, List<Map<String, Object>> patternDefinitions, List<Map<String, Object>> preconfiguredEventTypes, boolean stats, boolean useInternalPatterns) throws Exception {
		this(patternReferences, patternDefinitions, preconfiguredEventTypes, stats, useInternalPatterns, null);
	}
	
	public Parser(List<String> patternReferences, List<Map<String, Object>> patternDefinitions, List<Map<String, Object>> preconfiguredEventTypes, boolean stats, boolean useInternalPatterns, List<String> eventTypesToParse) throws Exception {
		
		List<PatternDefinition> patternDefinitionObjects = new ArrayList<PatternDefinition>();
		if(patternDefinitions != null ) {
			patternDefinitionObjects = patternDefinitions.stream().map( (Map<String, Object> obj) -> {
				try {
					return new PatternDefinition(obj);
				} catch(Exception e) {
					log.warn("could not create pattern definition, skipping this one");
					return null;
				}
			}).filter( it -> it != null).collect(Collectors.toList());
		}
		
		List<PatternDefinition> preconfiguredEventTypeObjects = new ArrayList<PatternDefinition>();
		if(preconfiguredEventTypes != null) {
			preconfiguredEventTypeObjects = preconfiguredEventTypes.stream().map( (Map<String, Object> obj) -> {
				try {
					return new PatternDefinition(obj);
				} catch(Exception e) {
					log.warn("could not create pattern definition, skipping this one");
					return null;
				}
			}).filter( it -> it != null).collect(Collectors.toList());
		}
		
		initInstance(patternReferences, patternDefinitionObjects, preconfiguredEventTypeObjects, stats, useInternalPatterns, null);
	}
	
	private void initInstance(List<String> patternReferences, List<PatternDefinition> patternDefinitions, List<PatternDefinition> preconfiguredEventTypes, boolean stats, boolean useInternalPatterns, List<String> eventTypesToParse) throws Exception {
		log = LoggerFactory.getLogger(Parser.class);
		// enable staticsts
		if(stats) {
			customStats = new CustomStats();
		}
		
		if(eventTypesToParse != null && eventTypesToParse.size() > 0 ) {
			this.eventTypesToParse = eventTypesToParse;
		}
		
		// initialize base grok and embedded patterns
		List<PatternDefinition> rawPatterns = initialize(useInternalPatterns);
		
		log.debug("read {} internal patterns", rawPatterns.size());
		
		if(preconfiguredEventTypes != null) {
			 this.preconfiguredEventTypes = preconfiguredEventTypes.stream().map( (Map<String, Object> obj) -> {
				try {
					return new PatternDefinition(obj);
				} catch(Exception e) {
					log.warn("could not create pattern definition, skipping this one");
					return null;
				}
			}).filter( it -> it != null).collect(Collectors.toList());
		}
		
		if(patternReferences != null && patternReferences.size() > 0) {
			List<PatternDefinition> tmpPatterns = readPatternReferences(patternReferences);
			rawPatterns.addAll(tmpPatterns);
			log.debug("read additional {} patterns from files, total {} patterns now", tmpPatterns.size(), rawPatterns.size());
		}
		
		if(patternDefinitions != null && patternDefinitions.size() > 0) { 
			rawPatterns.addAll(patternDefinitions);
			log.debug("added additional {} pattern definitions, total {} patterns now", patternDefinitions.size(), rawPatterns.size());
		}
		
		// if base-ptterns are still missing, add them
		if(rawPatterns.stream().filter( item -> item.getName().equals(DEFAULT_EVENT_TYPE)).count() == 0) {
			List<PatternDefinition> basePatterns = basePatterns();
			for(PatternDefinition baseItem : basePatterns) {
				if(rawPatterns.stream().filter( item -> item.getName().equals(baseItem.getName())).count() == 0) {
					rawPatterns.add(0, baseItem);
				};
			};
		}

		patterns = parsePatterns(rawPatterns);
	}
	
	public static class Builder {
		
		private List<String> patternReferences = new ArrayList<String>();
		private List<Map<String, Object>> patternDefinitionsAsMap = new ArrayList<Map<String, Object>>();
		
		private List<PatternDefinition> patternDefinitionObjects = new ArrayList<PatternDefinition>();
		
		private boolean keepStats = false;
		private boolean useInternalPatterns = true;
		
		public Builder() { }
		
		public Builder withPatternFile(String filePath) {
			patternReferences.add(filePath);
			return this;
		}
		
		public Builder withPatternFiles(List<String> filePaths) {
			patternReferences.addAll(filePaths);
			return this;
		}

		
		///////////////////////////
		//  pattern definitions  //
		///////////////////////////
		
		
		public Builder withPatternDefinitionAsMap(Map<String, Object> patternDefinition) {
			patternDefinitionsAsMap.add(patternDefinition);
			return this;
		}
		
		public Builder withPatternDefinitionAsMap(List<Map<String, Object>> patternDefinitions) {
			patternDefinitionsAsMap.addAll(patternDefinitions);
			return this;
		}
		
		public Builder withPatternDefinition(PatternDefinition patternDefinition) {
			patternDefinitionObjects.add(patternDefinition);
			return this;
		}
		
		public Builder withPatternDefinition(List<PatternDefinition> patternDefinitions) {
			patternDefinitionObjects.addAll(patternDefinitions);
			return this;
		}

		
		public Builder withStats() {
			this.keepStats = true;
			return this;
		}

		public Builder withoutInternalPatterns() {
			this.useInternalPatterns = false;
			return this;
		}
		
		public Parser build() throws Exception {
			
			patternDefinitionObjects.addAll(patternDefinitionsAsMap.stream().map( (Map<String, Object> obj) -> {
				try {
					return new PatternDefinition(obj);
				} catch(Exception e) {
					//log.warn("could not create pattern definition, skipping this one");
					return null;
				}
			}).filter( it -> it != null).collect(Collectors.toList()));
			
			Parser parser = new Parser();
			parser.initInstance(patternReferences, patternDefinitionObjects, null, keepStats, useInternalPatterns, null);
			return parser;
		}
	}
	
	public List<PatternDefinition> getPatterns() {
		return this.patterns;
	}

	public void prettyPrintRegs() {
		prettyPrintRegsRecursive(null, 1, false);
	}
	
	public void prettyPrintRegs(boolean withFields) {
		prettyPrintRegsRecursive(null, 1, withFields);
	}
	
	private void prettyPrintRegsRecursive(String parent, int level, boolean withFields) {
		
		if(parent == null) {
			patterns.stream()
			.filter( it -> {
				return !it.hasParents();
			})
			.sorted((m1, m2) -> {
			    return (m1.containsKey("order")?Integer.parseInt(m1.get("order").toString()):0) - (m2.containsKey("order")?Integer.parseInt(m2.get("order").toString()):0);
			})
			.peek( it -> {	
				/*String regex = "";
				if(withRegex && it.containsKey("pattern")) {
					if(it.get("pattern") instanceof String) {
						regex = " = " + ((Grok)it.get("pattern")).getNamedRegex();
					} else if (it.get("pattern") instanceof List && ((List)it.get("pattern")).size() == 1){
						regex = " = " + ((Grok)((List)it.get("pattern")).get(0)).getNamedRegex();
					} else {
						regex = "multiple regexes";
					}
				}*/
				String fields = "";
				if(withFields && it.containsKey("fields")) {
					if(it.get("fields") instanceof Map) {
						fields = " = " + ((Map<String, String>)it.get("fields")).keySet().stream().map(Object::toString).collect(Collectors.joining(","));
					}
				}

				System.out.println(new String(new char[level]).replace("\0", "+") + " " + it.getName() + fields);
				prettyPrintRegsRecursive(it.getName(), (level + 1), withFields);
			})
			.count();
		} else {
			patterns.stream()
			.filter( (it) -> {
				if(it.hasParents()) {
					return it.getParents().contains(parent);
				} else {
					return false;
				}
			})
			.sorted((m1, m2) -> {
			    return (m1.containsKey("order")?Integer.parseInt(m1.get("order").toString()):0) - (m2.containsKey("order")?Integer.parseInt(m2.get("order").toString()):0);
			})
			.peek( it -> {	
				String fields = "";
				
				/*
				String regex = "";
				if(withRegex && it.containsKey("pattern")) {
					if(it.get("pattern") instanceof String) {
						regex = " = " + ((Grok)it.get("pattern")).getNamedRegex();
					} else if (it.get("pattern") instanceof List && ((List)it.get("pattern")).size() == 1){
						regex = " = " + ((Grok)((List)it.get("pattern")).get(0)).getNamedRegex();
					} else {
						regex = "multiple regexes";
					}
				}*/
				if(withFields && it.containsKey("fields")) {
					if(it.get("fields") instanceof Map) {
						fields = " = " + ((Map<String, String>)it.get("fields")).keySet().stream().map(Object::toString).collect(Collectors.joining(","));
					}
				}
				System.out.println(new String(new char[level]).replace("\0", "+") + " " + it.getName() + fields);
				prettyPrintRegsRecursive(it.getName(), (level + 1), withFields);
			})
			.count();
		}
	}
	
	public CustomStats getStats() {
		return customStats;
	}

	public Map<String, Object> matchline(String line) {
		Map<String, Object> parsed = new HashMap<String, Object>();
		List<String> matchedRegs = new ArrayList<String>();
		parsed.put("__match", matchedRegs);
		parsed.put("data", line);
		matchline(parsed, null, true);
		return parsed;
	}

	public void matchline(String line, Map<String, Object> match) {
		List<String> matchedRegs = new ArrayList<String>();
		match.put("__match", matchedRegs);
		match.put("data", line);
		matchline(match, null, true);
	}
	
	private void matchline(Map<String, Object> match, String parent) {
		matchline(match, parent, false);
	}
	
	private void matchline(Map<String, Object> match, String parent, Boolean toplevel) {
		
		List<PatternDefinition> levelmaps;
		
		/** we collect all the patterns with given parent (so all the child patterns) and sort them **/
		if(parent == null) {
			
			levelmaps = patterns.stream()
				.filter( it -> !it.hasParents())
				.sorted((m1, m2) -> {
				    return (m1.containsKey("order")?Integer.parseInt(m1.get("order").toString()):0) - (m2.containsKey("order")?Integer.parseInt(m2.get("order").toString()):0);
				})
				//.peek( it -> System.out.println("name=" + it.getName() + "; order = " + it.get("order")))
				.collect(Collectors.toList());
		} else {
			levelmaps = patterns.stream()
				.filter( (it) -> {
					if(it.hasParents()) {
						return it.getParents().contains(parent);
					} else {
						return false;
					}
				})
				.sorted((m1, m2) -> {
				    return (m1.containsKey("order")?Integer.parseInt(m1.get("order").toString()):0) - (m2.containsKey("order")?Integer.parseInt(m2.get("order").toString()):0);
				})
				//.peek( it -> System.out.println("name=" + it.getName() + "; order = " + it.get("order")))
				.collect(Collectors.toList());
		}
		
		/** now we iterate over collected patterns and do the pattern matching **/
		for (PatternDefinition regexmap : levelmaps) {
		
			try {
				// if we have field level conditions, we check them
				if(regexmap.containsKey("cond") && regexmap.get("cond") != null) {
					Map<String, String> conditions = (Map<String, String>) regexmap.get("cond");
					
					// if we have at least one defined condition field in match that doesn't match, we skip this regexmap
					if(conditions.entrySet().stream().filter(item -> {
						if (!match.containsKey(item.getKey())) {
							// we dont have such field at all
							return true;
						} else if (item.getValue() == null || match.get(item.getKey()) == null) {
							// match or condition value is null - we return false ?!?
							return true;
						} else {
						      Pattern r = Pattern.compile(item.getValue());
						      Matcher m = r.matcher((String)match.get(item.getKey()));
						      return !m.matches();
						}
						//return (!match.containsKey(item.getKey())) || (match.containsKey(item.getKey()) && ( item.getValue() == null || match.get(item.getKey()) == null || !(match.get(item.getKey()).equals(item.getValue()))));
					}).count() > 0) {
						continue;
					}
					
				}
				
				// if we have field level SOFT conditions, we check them (SOFT conditions only apply if data object has this field)
				if(regexmap.containsKey("softcond") && regexmap.get("softcond") != null) {
					Map<String, String> conditions = (Map<String, String>) regexmap.get("softcond");
					
					// we remove the need for condition field to exist
					if(conditions.entrySet().stream().filter(item -> {
						if (match.containsKey(item.getKey()) && match.get(item.getKey()) != null) {
						      Pattern r = Pattern.compile(item.getValue());
						      Matcher m = r.matcher((String)match.get(item.getKey()));
						      return !m.matches();
						} else {
							return false;
						}
						//return (match.containsKey(item.getKey()) && !(match.get(item.getKey()).equals(item.getValue())));
					}).count() > 0) {
						continue;
					}
					
				}
				
				String matchField;
				if(regexmap.containsKey("field") && regexmap.get("field") != null) {
					matchField = regexmap.get("field").toString();
				} else {
					matchField = "data";
				}
				
				if(!match.containsKey(matchField) || match.get(matchField) == null) {
					continue;
				}
	
				boolean anyMatch = false;
				
				if (regexmap.containsKey("pattern")) { // we handle grok here
					
					List<Grok> patterns = new ArrayList<Grok>();
					
					if (regexmap.get("pattern") instanceof Grok) {
						patterns.add((Grok)regexmap.get("pattern"));
					} else if (regexmap.get("pattern") instanceof List){
						patterns.addAll((List<Grok>)regexmap.get("pattern"));
					}
					
					for(Grok g : patterns) {
						
						Match gm = g.match(match.get(matchField).toString());
						
						if(gm.getMatch() != null && gm.getMatch().matches()) {
							anyMatch = true;
							((List)match.get("__match")).add(regexmap.getName());
							match.putAll(gm.capture());
						}
					}
					
					if (anyMatch) {
						// handle additional (optional) field matches if line is already matched	
						
						if(customStats != null) {
							customStats.inc("matchcount", regexmap.getName());
						}
						
						patterns = new ArrayList<Grok>();
						if (regexmap.get("optionalpattern") instanceof Grok) {
							patterns.add((Grok)regexmap.get("optionalpattern"));
						} else if (regexmap.get("optionalpattern") instanceof List){
							patterns.addAll((List<Grok>)regexmap.get("optionalpattern"));
						}
						
						for(Grok g : patterns) {
							
							Match gm = g.match(match.get(matchField).toString());
							
							/* NO STATS HERE RIGHT?
							 * if(customStats != null) { N
								customStats.inc("matchcount", regexmap.getName());
							}*/
							
							if(gm.getMatch() != null && gm.getMatch().matches()) {
								match.putAll(gm.capture());
							}
						}
						// end additional field matching
						
						// TODO: HOW THE FUCK DO I HANDLE THIS?
						/*if(!(Boolean)regexmap.get("keepfield")) {
							match.remove(matchField);
						}
						
						// like SO ??!
						if((Boolean)regexmap.get("removedata")) {
							match.remove(matchField);
						}*/
						
						((Map<String, Object>)regexmap.get("fields")).forEach( (name, type) -> {
							try {
								if(type instanceof String && ((String)type).equals("int") && match.containsKey(name)) {
									match.put((String) name, Integer.parseInt(match.get(name).toString()));
								}
							} catch (Exception e) {
								log.warn("could not parse value '{}' to int (type: {}, field: {})", match.get(name).toString(), regexmap.getName(), name);
							}
							
							try {
								if(type instanceof String && ((String)type).equals("long") && match.containsKey(name)) {
									match.put((String) name, Long.parseLong(match.get(name).toString()));
								}
							} catch (Exception e) {
								log.warn("could not parse value '{}' to long (type: {}, field: {})", match.get(name).toString(), regexmap.getName(), name);
							}
							
							try {
								if(type instanceof String && ((String)type).equals("float") && match.containsKey(name)) {
									match.put((String) name, Float.parseFloat(match.get(name).toString()));
								}
							} catch (Exception e) {
								log.warn("could not parse value '{}' to float (type: {}, field: {})", match.get(name).toString(), regexmap.getName(), name);
							}
							
							try {
								if(type instanceof String && ((String)type).equals("double") && match.containsKey(name)) {
									match.put((String) name, Double.parseDouble(match.get(name).toString()));
								}
							} catch (Exception e) {
								log.warn("could not parse value '{}' to double (type: {}, field: {})", match.get(name).toString(), regexmap.getName(), name);
							}
							/* else if(type instanceof Class && ((Class)type).equals(IP.class) && match.containsKey(name)) {
								try {
									match.put((String) name, new IP(match.get(name).toString()));
								} catch (UnknownHostException e) {
									match.put((String) name, match.get(name).toString());
									System.err.println(match.get(name).toString() + "; err: " + e.getMessage());
								}
							}*/
						});
						
						if(customStats != null) {
							customStats.inc("matches", regexmap.getName());
						}
						
						matchline(match, regexmap.getName());
					} else {
						if(customStats != null) {
							customStats.inc("nonmatches", regexmap.getName());
						}
					}
				}
				
				// we have a match, so we handle extra transformation requests (should we do that before nested parsing calls ????? ))
				if(anyMatch) {
					
					try {
						
						// kui on defineeritud et mingi leitud v2li sisaldab jsonit, siis kontrollime kas see väli on olemas ja proovime jsoni välja parsida
						if(regexmap.get("json") != null && match.containsKey(regexmap.get("json").toString()) && match.get(regexmap.get("json").toString()) != null) {
							if(customStats != null) {
								customStats.inc("desercount", regexmap.getName());
							}
							Map<String, Object> obj = JsonIterator.deserialize(match.get(regexmap.get("json")).toString(), new TypeLiteral<Map<String, Object>>(){});
	
							
							if(regexmap.containsKey("json.nested")) {
								// add json as new field 
								match.put(regexmap.get("json").toString(), obj);
							} else {
								// remove raw json string field
								match.remove(regexmap.get("json").toString());
								
								// merge json into match object
								obj.forEach( (key, value) -> {
									match.put(key, value);
								});
							}
							
						}
						// if there should be a csv formatted data in some field, then try to parse it
						if(regexmap.get("csv") != null && match.containsKey(regexmap.get("csv").toString()) && match.get(regexmap.get("csv").toString()) != null) {
							int i=0;
							String[] parseddata = csvParser.parseLine(match.get(regexmap.get("csv")).toString());
							for(String field : (List<String>)regexmap.get("csv-fields")) {
								if(parseddata.length > i) {
									match.put(field.replaceAll("-", "_"), parseddata[i]);
								}
								i++;
							}
							//split​(String regex)
							//System.out.println(match.get(regexmap.get("csv")).toString());
							//System.out.println("ARR:");
							/*for(String token : csv.parseLine(match.get(regexmap.get("csv")).toString())) {
								System.out.print(token + "; ");
							}*/
							//System.out.println("==");
						}
						
						// kui on defineeritud et mingi v2li sisaldab url argumente, siis parsime need lahti
						if(regexmap.get("urlargs") != null && match.containsKey(regexmap.get("urlargs").toString()) && match.get(regexmap.get("urlargs").toString()) != null) {
							try {
								Map<String, String> obj = splitArgs(match.get(regexmap.get("urlargs")).toString());
								if(regexmap.containsKey("urlargs.nested")) {
									// add json as new field 
									match.put(regexmap.get("urlargs").toString(), obj);
								} else {
									// remove original match field
									match.remove(regexmap.get("urlargs").toString());
									
									// merge json into match object
									obj.forEach( (key, value) -> {
										match.put(key, value);
									});
								}
							} catch (UnsupportedEncodingException e) {
								// TODO Auto-generated catch block
								match.put(regexmap.get("urlargs").toString(), new HashMap<String, String>());
								//e.printStackTrace();
							}
						}
						
						// if replace has been defined in a field, try to execute it
						if(regexmap.get("replace") != null && regexmap.get("replace") instanceof List) {
							((List)regexmap.get("replace")).forEach( replaceObject -> {
								if(
										replaceObject instanceof Map &&
										((Map)replaceObject).containsKey("field") && 
										((Map)replaceObject).containsKey("regex") && 
										((Map)replaceObject).containsKey("replacement") && 
										match.containsKey(((Map)replaceObject).get("field"))) {
									String field = ((Map)replaceObject).get("field").toString();
									String result = match.get(field).toString().replaceAll(((Map)replaceObject).get("regex").toString(), ((Map)replaceObject).get("replacement").toString());
									match.put(field, result);
								}
							});
						}
						
						// src timestamp parsing
						if(regexmap.containsKey("srctime-field") && regexmap.get("srctime-field") instanceof String && match.containsKey((String)regexmap.get("srctime-field")) && regexmap.containsKey("srctime-format-object") && regexmap.get("srctime-format-object") instanceof SimpleDateFormat) {
							
						    try {
						    	SimpleDateFormat dateFormat = (SimpleDateFormat)regexmap.get("srctime-format-object");
								Date parsedDate = dateFormat.parse((String)match.get((String)regexmap.get("srctime-field")));
								match.put("src_logts_timestamp", parsedDate.getTime());
							} catch (ParseException e) {
								// pass
							}
						}
							
						
					} catch (JsonException e) {
						// json parsing failed, so we make the assumption that this matcher failed and we keep trying with others
						continue;
					}
					break;
				}
			} catch (Exception e) {
				// catch possible nullpointers ? 
				System.err.println("smth happened during one of the matching iterations : " + e.getMessage());
				e.printStackTrace();
			}
		}

		// this we only execute once per line being matched
		if(toplevel) {
			 // we add default type if somehow nothing exists.. this should not acutally happen
			if(!match.containsKey("__match")) {
				match.put("__match", new ArrayList<Object>());
			}
			
			if(((List)match.get("__match")).size() == 0) {
				((List)match.get("__match")).add(CometApplication.DEFAULT_EVENT_TYPE);
			}
			
			match.put("eventType", (String)((List)match.get("__match")).get(((List)match.get("__match")).size()-1));
		}
	}

	private List<PatternDefinition> readPatternReferences() throws Exception {
		return readPatternReferences(null);
	}

	private List<PatternDefinition> basePatterns() {
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream("cometpatterns/000_base.yaml");
		Map<String, Object> rawObj;
		if(inputStream != null) {
			Yaml yaml = new Yaml();
			rawObj = yaml.load(inputStream);
			List<PatternDefinition> regList = new ArrayList<PatternDefinition>();
			regList = ((List<Map<String, Object>>)rawObj.get("patterns")).stream().map((Map<String, Object> pat) -> {
				try {
					return new PatternDefinition(pat);
				} catch(ParserException e) {
					return null;
				}
			}).filter( pat -> pat != null).collect(Collectors.toList());
			
			rawObj.put("patterns", regList);
		} else { // this is for graal-vm, at least for now... 
			
			// create events reg item
			PatternDefinition item = new PatternDefinition(MonitorRuntimeConfiguration.DEFAULT_EVENT_TYPE);
			item.put("order", 1);
			item.put("keepfield", true);
			item.put("pattern", "(?<data>.*)");
			
			// create list of regs, and add events item
			List<PatternDefinition> regList = new ArrayList<PatternDefinition>();
			regList.add(item);
			
			rawObj = new HashMap<String, Object>();
			rawObj.put("patterns", regList);
		}
		return (List<PatternDefinition>) rawObj.get("patterns");
	}
	
	private List<PatternDefinition> initialize(boolean useInternalPatterns) {

		log = LoggerFactory.getLogger(Parser.class);
		log.debug("Initializing patterns");
		
		List<PatternDefinition> rawPatterns = new ArrayList<PatternDefinition>();
		
		// fill grok compiler with default patterns
		try {
			gc.registerDefaultPatterns();
		} catch (Exception e) {
			log.warn("Registering default grok patterns failed");
			log.debug("Registering default grok patterns failed", e);
		}
		
		try {
			InputStream is = getClass().getClassLoader().getResourceAsStream("grok-patterns.txt");
			if(is != null) {
				gc.register(is);
				log.debug("Registered default grok patterns");
			}
		} catch (IOException e) {
			log.warn("Registering custom grok patterns failed : " + e.getMessage());
			log.debug("Registering custom grok patterns failed : " + e.getMessage(), e);
		}
		
		if(useInternalPatterns) {
			rawPatterns.addAll(readInternalDefinitions());
		}
		return rawPatterns;
	}
	
	private List<PatternDefinition> readPatternReferences(List<String> patternFiles) {
		
		List<InputStream> patternInputStreams = new ArrayList<InputStream>();
		List<PatternDefinition> patternDefinitions = new ArrayList<PatternDefinition>();
		
		
		// iterate over the source list
		if(patternFiles != null && patternFiles.size() > 0) {

			Map<String, String> rawNamedTextPatterns = new HashMap<String, String>();
			
			for(String patternFilename : patternFiles) {
				if(patternFilename == null) {
					continue;
				}
				
				File patternFile = new File(patternFilename);
				
				
				// if input is a file, then parse it later on
				if(patternFile.exists()) {
					try {
						patternInputStreams.add(new FileInputStream(patternFile));
						log.debug("loaded patternfile - " + patternFilename);
					} catch (FileNotFoundException e) {
						log.warn("No pattern file '" + patternFilename +"' found. Continuing startup without these patterns.");
					}
					
				// if plain text pattern (named or just pattern)
				} else {
					log.error("no pattern-file '{}' found", patternFile.getAbsolutePath());
				}
			}
		}
		
		// extract patterns and grok definitsions from patterndef files
		if(patternInputStreams != null && patternInputStreams.size() > 0) {
			for (InputStream patternInputStream : patternInputStreams) {
				Yaml yaml = new Yaml();
				Map<String, Object> rawObj = yaml.load(patternInputStream);
				if(rawObj.containsKey("grok")) {
					Map<String, String> grokPatterns = (Map<String, String>) rawObj.get("grok");
					registerGrokPatterns(grokPatterns);
				}
				
				if (rawObj.containsKey("patterns")) {
					patternDefinitions.addAll(((List<Map<String, Object>>)rawObj.get("patterns")).stream().map( (Map<String, Object> obj) -> {
						try {
							return new PatternDefinition(obj);
						} catch(Exception e) {
							log.warn("could not create pattern definition, skipping this one");
							return null;
						}
					}).filter( it -> it != null).collect(Collectors.toList()));
				}
			}
		}
		
		return patternDefinitions;
	}
	
	/**
	 * grok statements will be registered during read 
	 * 
	 * @return
	 */
	private List<PatternDefinition> readInternalDefinitions() {

		List<InputStream> patternInputStreams = new ArrayList<InputStream>();
		List<PatternDefinition> patternDefinitions = new ArrayList<PatternDefinition>();
		
		CodeSource src = CometApplication.class.getProtectionDomain().getCodeSource();
		if(src != null) {
		URL embeddedPatterns = src.getLocation();
			ZipInputStream zip;
			try {
				zip = new ZipInputStream(embeddedPatterns.openStream());
				while(true) {
					ZipEntry e = zip.getNextEntry();
					if (e == null)
						break;
					String name = e.getName();
					if (name.matches("^cometpatterns/[0-9]+.*\\.yaml")) {
						patternInputStreams.add(getClass().getClassLoader().getResourceAsStream(name));
				    }
				}
			} catch (IOException e) {
				log.warn("Could not load default patterns. Starting without them. Cause :" + e.getMessage());
				log.debug("Could not load default patterns. Starting without them. Cause :" + e.getMessage(), e);
			}
		}
		
		if(patternInputStreams != null && patternInputStreams.size() > 0) {
			for (InputStream patternInputStream : patternInputStreams) {
				Yaml yaml = new Yaml();
				Map<String, Object> rawObj = yaml.load(patternInputStream);
				if(rawObj.containsKey("grok")) {
					Map<String, String> grokPatterns = (Map<String, String>) rawObj.get("grok");
					registerGrokPatterns(grokPatterns);
				}
				
				if (rawObj.containsKey("patterns")) {
					patternDefinitions.addAll(((List<Map<String, Object>>)rawObj.get("patterns")).stream().map( (Map<String, Object> obj) -> {
						try {
							return new PatternDefinition(obj);
						} catch(Exception e) {
							log.warn("could not create pattern definition, skipping this one");
							return null;
						}
					}).filter( it -> it != null).collect(Collectors.toList()));
				}
			}
		}
		
		return patternDefinitions;
	}
	
	/**
	 * Iterate over loaded pattern definitions and precalculate/precompile things. This is called as final step during Parser creation.
	 * 
	 * @param rawRegs
	 * @return
	 */
	private List<PatternDefinition> parsePatterns(List<PatternDefinition> rawPatternDefinitions) {
		
		// check if somehow base patterns are still missing. if they are, add them.
		if(rawPatternDefinitions.stream().filter( item -> item.getName().equals(DEFAULT_EVENT_TYPE)).count() == 0) {
			List<PatternDefinition> basePatterns = basePatterns();
			for(PatternDefinition baseItem : basePatterns) {
				if(rawPatternDefinitions.stream().filter( item -> item.getName().equals(baseItem.getName())).count() == 0) {
					rawPatternDefinitions.add(0, baseItem);
				};
			};
		}
		
		List<String> complexFields = Arrays.asList("json", "urlargs", "csv");
		
		List<PatternDefinition> patternDefinitions = rawPatternDefinitions.stream().map(it -> {
			
			// turn list of strings into valid field definitions with default type string
			if(it.containsKey("fields") && it.get("fields") instanceof List && ((List)it.get("fields")).size() > 0 && ((List)it.get("fields")).get(0) instanceof String) {
				Map<String, String> tmpMap = new HashMap<String, String>();
				for(String field : (List<String>)it.get("fields")) {
					tmpMap.put(field.replaceAll("-", "_"), "string");
				}
				it.put("fields", tmpMap);
			}
			
			// turn csv fields into valid fields with string type 
			if(it.containsKey("csv-fields") && it.get("csv-fields") instanceof List && ((List)it.get("csv-fields")).size() > 0 && ((List)it.get("csv-fields")).get(0) instanceof String) {
				Map<String, String> tmpMap = new HashMap<String, String>();
				for(String field : (List<String>)it.get("csv-fields")) {
					tmpMap.put(field.replaceAll("-", "_"), "string");
				}
				it.put("fields", tmpMap);
			}
			
			if(it.containsKey("pattern")) {
				try {

					List<String> patterns = new ArrayList<String>();
					if(it.containsKey("pattern") && it.get("pattern") instanceof String) {
						patterns.add((String)it.get("pattern"));
					} else if (it.containsKey("pattern") && it.get("pattern") instanceof List) {
						patterns.addAll((List<String>)it.get("pattern"));
					}

					List<String> optionalPatterns = new ArrayList<String>();
					if(it.containsKey("optionalpattern") && it.get("optionalpattern") instanceof String) {
						optionalPatterns.add((String)it.get("optionalpattern"));
					} else if (it.containsKey("optionalpattern") && it.get("optionalpattern") instanceof List) {
						optionalPatterns.addAll((List<String>)it.get("optionalpattern"));
					}
					
					// TODO CALL SHIT
					List<Grok> grokPatterns = parseGrokPatterns(patterns);
					List<Grok> optionalGrokPatterns = parseGrokPatterns(optionalPatterns);
					
					Map<String, String> fields = parseFieldsFromGrokPatterns(grokPatterns);
					fields.putAll(parseFieldsFromGrokPatterns(optionalGrokPatterns));
					
					for(Map.Entry<String,String> entry : fields.entrySet()) {
						// TODO! this solution supports only one json field (currently first only), SUCK SUCK SUCK
						for (String complexField : complexFields) {
							if(entry.getValue().toLowerCase().startsWith(complexField)) {
								it.put(complexField, entry.getKey());
								if(entry.getValue().toLowerCase().equals(complexField + ".nested")) {
									it.put(complexField + ".nested", entry.getKey());
								}
								fields.put(entry.getKey(), "string");
								break;
							}
						};
						// TODO! this solution supports only one urlargs field (currently first only), SUCK SUCK SUCK
						if(entry.getValue().toLowerCase().startsWith("json")) {
							it.put("json", entry.getKey());
							fields.put(entry.getKey(), "string");
							break;
						}
						if(entry.getValue().toLowerCase().startsWith("csv")) {
							it.put("csv", entry.getKey());
							fields.put(entry.getKey(), "string");
							break;
						}
						if(entry.getValue().toLowerCase().startsWith("urlargs")) {
							it.put("urlargs", entry.getKey());
							fields.put(entry.getKey(), "string");
							break;
						}
					}
					if(it.containsKey("fields") && it.get("fields") instanceof Map) {
						fields.putAll((Map<String,String>)it.get("fields"));
					}
					it.put("fields", fields);
					it.put("pattern", grokPatterns);
					it.put("optionalpattern", optionalGrokPatterns);

				} catch (GrokException e) {
					log.warn("Grok error during pattern parsing: " + e.getMessage());
					log.debug("Grok error during pattern parsing: " + e.getMessage(), e);
				} catch (Exception e) {
					log.warn("General error during pattern parsing: " + e.getMessage());
					log.debug("General error during pattern parsing: " + e.getMessage(), e);
				} finally {
					if(!it.containsKey("fields")) {
						it.put("fields", new HashMap<String, String>());
					}
				}
			}
			
			// change complex field type values 
			if(it.containsKey("fields")) {
				((Map<String, String>)it.get("fields")).forEach((fieldname, index) -> {
					if( ((Map<String, Object>)it.get("fields")).get(fieldname).equals("map") ) {
						((Map<String, Object>)it.get("fields")).put(fieldname, java.util.Map.class);
					}
				});
			}
			
			// precompile src time pattern
			if(it.containsKey("srctime-format") && it.get("srctime-format") instanceof String) {
				try {
					SimpleDateFormat dateFormat = new SimpleDateFormat((String)it.get("srctime-format"));
					it.put("srctime-format-object", dateFormat);
					((HashMap<String,Object>)it.get("fields")).put("src_logts_timestamp", Long.class);
				} catch (Exception e) {
					log.warn("srctime format '{}' is invalid for event '{}'. error: {}", (String)it.get("srctime-format"), it.getName(), e.getMessage());
				}
			}
			
			it.put("keepfield", (it.containsKey("keepfield") && (boolean)it.get("keepfield")));
			
			// if no parent, set events as parent
			if(!it.hasParents() && !it.getName().equals(MonitorRuntimeConfiguration.DEFAULT_EVENT_TYPE)) {
				it.setParent(MonitorRuntimeConfiguration.DEFAULT_EVENT_TYPE);
			}
			
			return it;
		}).sorted((Map m1, Map m2) -> {
			if(!m1.containsKey("order") && !m2.containsKey("order")) {
				return 0;
			} else if(!m1.containsKey("order") && m2.containsKey("order")) {
				return 1;
			} else if (m1.containsKey("order") && !m2.containsKey("order")) {
				return -1;
			} else {
				return (m1.containsKey("order")?Integer.parseInt(m1.get("order").toString()):0) - (m2.containsKey("order")?Integer.parseInt(m2.get("order").toString()):0);
			}
		}).collect(Collectors.toList());
		
		// add parent fields
		try {
			List<PatternDefinition> tmp = new ArrayList<>(patternDefinitions);
			patternDefinitions = patternDefinitions.stream().map((item) -> {	
	            item.put("parentFields", parentFields(item, tmp));
	            return item;
	        }).collect(Collectors.toList());
		} catch (Exception e) {
			log.warn("Adding parent fields to event types failed: " + e.getMessage());
			log.debug("Adding parent fields to event types failed: " + e.getMessage(), e);
		}
		
		// add preconfigured event types also to the mix (if they do not exist already)
		// why did i implement a secondary list anyway ???
		if(this.preconfiguredEventTypes != null && this.preconfiguredEventTypes.size() > 0) {
			for(PatternDefinition preEvent : preconfiguredEventTypes) {
				if(patternDefinitions.stream().filter( (PatternDefinition reg) -> reg.getName().equals(preEvent.getName()) ).count() == 0) {
					// this event has not been discovered yet, so lets add
					log.debug("adding preconfigured event type '{}'", preEvent.getName());
					patternDefinitions.add(preEvent);
				} else {
					log.debug("preconfigured event type '{}' has already been added from pattern file, skipping", preEvent.getName());
				}
			};
		}
		

		// add parent names as list (recursively to the top)
		List<PatternDefinition> tmp2 = new ArrayList<>(patternDefinitions);
		
		patternDefinitions = patternDefinitions.stream().map(item -> {
            item.put("parentsToTop", addParentList(item.getName(), new ArrayList<>(), tmp2));
            try {
            	return new PatternDefinition(item);
            } catch (Exception e) {
            	log.warn("pattern name cannot be null, ignoring this pattern");
            	return null;
            }
        }).filter(it -> it != null).collect(Collectors.toList());
		
		//regs = null;
		
		// now, all the pre-calculation should be done
		
		// if there is a list of events that we should only use for parsing, then do this filtering now
		if(eventTypesToParse != null && eventTypesToParse.size() > 0) {
			log.debug("remove all event types from parser that are not these (or their parents): {}", eventTypesToParse);
			Set<String> eventTypesToKeep = new HashSet<String>(eventTypesToParse);
			patternDefinitions.forEach( (PatternDefinition pattern) -> {
				String eventName = pattern.getName();
				if(eventTypesToParse.contains(eventName) && pattern.containsKey("parentsToTop") && pattern.get("parentsToTop") instanceof List) {
					eventTypesToKeep.addAll((List<String>)pattern.get("parentsToTop"));
				}
			});
			patternDefinitions = patternDefinitions.stream().filter( pattern -> eventTypesToKeep.contains(pattern.getName())).collect(Collectors.toList());
		}
		return patternDefinitions;
	}
	
	private List<Grok> parseGrokPatterns(List<String> patterns) throws Exception {
		Map<String, Object> retMap = new HashMap<String, Object>();
		List<Grok> grokPatterns = new ArrayList<Grok>();
		
		for (String pat : patterns) {
			try {
				final Grok g = gc.compile(pat, true);
				grokPatterns.add(g);
			} catch (Exception e) {
				log.warn("Parsing grok pattern failed, skipping this one. Pattern: \"" + pat + "\". Reason: " + e.getMessage());
			}
		}
		
		return grokPatterns;
	}
	
	private Map<String, String> parseFieldsFromGrokPatterns(List<Grok> grokPatterns) throws Exception {

		
		// Ugly fucking hack
		Map<String, String> grokToEsperType = new HashMap<String, String>() {{
			put("INT", "int");
			put("CSV", "csv");
			put("CSV_NESTED", "csv.nested");
			put("JSON", "json");
			put("JSON_NESTED", "json.nested");
			put("URLARGS", "urlargs");
			put("URLARGS_NESTED", "urlargs.nested");
			//put("IPV4", "IPV4");
			//put("IPNULLNOTCOMMA", "java.net.InetAddress");
		}};
		
		
		Map<String, String> fields = new HashMap<String, String>();
		
		for (Grok g : grokPatterns) {

			String pat = g.getOriginalGrokPattern();
			// we collect all grok patterns, so we could auto-cast some grok types to esper types 
			Matcher matcher = GrokUtils.GROK_PATTERN.matcher(pat);
			Map<String, String> grokFields = new HashMap<String,String>();
			while(matcher.find()) {
				Map<String,String> namedGroups = GrokUtils.namedGroups(matcher, GrokUtils.getNameGroups(GrokUtils.GROK_PATTERN.pattern()));
				if(namedGroups.containsKey("pattern") && namedGroups.containsKey("subname") && namedGroups.get("subname") != null) {
					grokFields.put(namedGroups.get("subname"), namedGroups.get("pattern"));
				}
			}
			
			g.getNamedRegexCollection().forEach((regexName, fieldname) -> {
				if( ! fields.containsKey(fieldname) ) {
					String esperType = "string";
					if(grokFields.containsKey(fieldname) && grokToEsperType.containsKey(grokFields.get(fieldname).toUpperCase())) {
						esperType = grokToEsperType.get(grokFields.get(fieldname).toUpperCase());
					}
					fields.put(fieldname, esperType);
				}
			});
			
			// now add fields that are manually defined as regex named groups
			try {
				fields(Pattern.compile(g.getNamedRegex())).forEach((fieldname, index) -> {
					if( ! fields.containsKey(fieldname) ) {
						fields.put(fieldname, "string");
					}
				});
			} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
				log.error("adding manually defined regex named groups failed : {}", e.getMessage());
			}
		}
		return fields;
	}
	
	private void registerGrokPatterns(Map<String, String> grokPatterns) {
		if(grokPatterns != null) {
			gc.register(grokPatterns);
		}
	}
	
	private List<String> addParentList(String entry, List<String> parents, List<PatternDefinition> regs) {
		regs.stream()
			.filter( it -> it.getName().equals(entry) && it.hasParents())
			.forEach(it -> {
				it.getParents().forEach( parent -> {
					if(!parents.contains(parent)) {
						parents.add(parent);
					}
					addParentList(parent, parents, regs);
				});
			});
		return parents;
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Integer> fields(Pattern pat) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final java.lang.reflect.Field namedGroups = pat.getClass().getDeclaredField("namedGroups");
		namedGroups.setAccessible(true);
		Map<String, Integer> ret = (Map<String, Integer>) namedGroups.get(pat);
		if(ret == null) {
			ret = new HashMap<String, Integer>();
		}
		return ret;
	}
	
	private List<String> parentFields(PatternDefinition regobj, List<PatternDefinition> regs) {
		//Map regobj = regs.find { it.name == reg }
		List<String> fields = new ArrayList<String>();
		if( regobj != null ) {
			regs.stream().filter((it) -> {
				if (regobj.getParents().contains(it.getName())) {
					return true;
				}
				return false;
			}).forEach((item) -> {
				fields.addAll(((Map)item.get("fields")).keySet());
				if(item.hasParents()) {
					fields.addAll(parentFields(item, regs));
				}
			});
		}
		return fields;
	}

	public static Map<String, String> splitArgs(String args) throws UnsupportedEncodingException {
		final Map<String, String> query_pairs = new LinkedHashMap<String, String>();
		final String[] pairs = args.split("&");
		for (String pair : pairs) {
			final int idx = pair.indexOf("=");
			final String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
			
			final String value = idx > 0 && pair.length() > idx + 1
					? URLDecoder.decode(pair.substring(idx + 1), "UTF-8")
					: null;
			query_pairs.put(key, value);
		}
		return query_pairs;
	}
	
	public static Map<String, List<String>> splitArgsMulti(String args) throws UnsupportedEncodingException {
		final Map<String, List<String>> query_pairs = new LinkedHashMap<String, List<String>>();
		final String[] pairs = args.split("&");
		for (String pair : pairs) {
			final int idx = pair.indexOf("=");
			final String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
			if (!query_pairs.containsKey(key)) {
				query_pairs.put(key, new LinkedList<String>());
			}
			final String value = idx > 0 && pair.length() > idx + 1
					? URLDecoder.decode(pair.substring(idx + 1), "UTF-8")
					: null;
			query_pairs.get(key).add(value);
		}
		return query_pairs;
	}
	
}
