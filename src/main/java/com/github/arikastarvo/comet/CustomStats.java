package com.github.arikastarvo.comet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CustomStats {

	private Map<String, Map<String, Integer>> stats = new HashMap<String, Map<String, Integer>>();
	
	public CustomStats() {
		Arrays.asList("matchcount", "desercount", "matches", "nonmatches").forEach(it -> stats.put(it, new HashMap<String, Integer>()));
	}
	
	public Integer set(String target, String level, Integer value) {
		stats.get(target).put(level, value);
		return value;
	}
	public Integer get(String target, String level) {
		return stats.get(target).get(level);
	}
	
	public Integer inc(String target, String level) {
		if(!(stats.get(target).containsKey(level)) || stats.get(target).get(level) == null) {
			stats.get(target).put(level, 0);
		}
		stats.get(target).put(level, stats.get(target).get(level) + 1);
		
		return stats.get(target).get(level);
	}
	
	public Map<String, Map<String, Integer>> stats() {
		return this.stats;
	}
	
}
