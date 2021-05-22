package com.github.arikastarvo.comet.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO
 * 
 * 
 * @author tarvo
 *
 */
public class PatternDefinition extends HashMap<String, Object> {

	/**
	 * name of the pattern
	 */
	private String name;
	
	/**
	 * names of parent patterns
	 */
	private List<String> parent = new ArrayList<String>();
	
	/**
	 * this is for transition period, should be removed after
	 * 
	 * @throws ParserException if smth went wrong
	 * @param data Pattern definition as it's in config file
	 */
	public PatternDefinition(Map<String, Object> data) throws ParserException {
		super(data);
		if(!data.containsKey("name")) {
			throw new ParserException("pattern definition must declare name");
		}
		
		// pre-set fields from map
		this.name = (String)data.get("name");
		
		/*if(data.containsKey("parent")) {
			if(data.get("parent") instanceof String) {
				this.parent.add((String)data.get("parent"));
			} else if (data.get("parent") instanceof List) {
				this.parent.addAll((List)data.get("parent"));
			}
		}*/
	}
	
	public PatternDefinition(String name) {
		super();
		this.name = name;
		put("name", name);
	}
	
	public static class Builder {
		private PatternDefinition patternDefinition;
		
		public Builder(String name) {
			this.patternDefinition = new PatternDefinition(name);
		}
		
		public Builder withParent(String parent) {
			//this.patternDefinition.parent.add(parent);
			this.patternDefinition.fixParents();
			((List<String>)this.patternDefinition.get("parent")).add(parent);
			return this;
		}
		
		public Builder withParents(List<String> parents) {
			//this.patternDefinition.parent.addAll(parents);
			this.patternDefinition.fixParents();
			((List<String>)this.patternDefinition.get("parent")).addAll(parents);
			return this;
		}
		
		public Builder withPattern(String pattern) {
			if(!patternDefinition.containsKey("pattern")) {
				patternDefinition.put("pattern", new ArrayList<String>());
			}
			((List<String>)patternDefinition.get("pattern")).add(pattern);
			return this;
		}
		
		public Builder withSourceField(String sourceField) {
			patternDefinition.put("field", sourceField);
			return this;
		}
		
		public PatternDefinition build() {
			return this.patternDefinition;
		}
	}
	
	
	public String getName() {
		return (String)super.get("name");
	}
	
	
	private void fixParents() {
		if(!super.containsKey("parent")) {
			super.put("parent", new ArrayList<String>());
		} else if(!(super.get("parent") instanceof List)) {
			String tmp = (String)super.get("parent");
			super.put("parent", new ArrayList<String>() {{ add(tmp); }});
		}
	}
	
	public boolean hasParents() {
		if(!super.containsKey("parent")) {
			return false;
		} else {
			if(super.get("parent") instanceof List && ((List)super.get("parent")).size() == 0) {
				return false;
			}
		}
		return true;
	}
	
	public List<String> getParents() {
		this.fixParents();
		if(super.get("parent") instanceof String) {
			return Arrays.asList((String)super.get("parent"));
		} else {
			return (List<String>)super.get("parent");
		}
	}
	
	public void setParents(List<String> parents) {
		super.put("parent", parents);
	}
	
	public void setParent(String parent) {
		super.put("parent", Arrays.asList(parent));
	}
	
	public void addParents(List<String> parents) {
		this.fixParents();
		((List<String>)super.get("parent")).addAll(parents);
	}
	
	public void addParent(String parent) {
		this.fixParents();
		((List<String>)super.get("parent")).add(parent);
	}
	
	public String getField() {
		return (String)super.get("field");
	}
	
	
	/**
	 * this is necessary for transition period 
	 * 
	 * @return Builder object
	 */
	public Map<String, Object> getAsMap() {
		return this;
	}
	

	
	/**
	 * this is necessary for transition period 
	 * 
	 * @return Builder object
	 */
	public Object get(String field) {
		
		switch(field) {
			case "namex":
				return getName();
			/*case "parent":
				return getParents();*/
			default:
				return super.get(field);
		}
		
	}

	
	/**
	 * this is necessary for transition period 
	 * 
	 * @return Builder object
	 */
	public Object put(String field, Object value) {
		
		switch(field) {
			case "namex":
				name = (String)value;
				return null;
				
			/*case "parent":
				parent = (List<String>)value;
				return null;*/
				
			default:
				return super.put(field, value);
		}
		
	}
}
