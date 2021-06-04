package com.github.arikastarvo.comet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.github.arikastarvo.comet.parser.Parser;
import com.github.arikastarvo.comet.parser.PatternDefinition;

public class ParserTest {
    
    @Test
    public void testBaseInit() throws Exception {

    	Parser parser = new Parser();
    	assertEquals(2, parser.getPatterns().size(), "there should be 2 basepatterns");
    	
    	parser = new Parser.Builder().build();
    	assertEquals(2, parser.getPatterns().size(), "there should be 2 basepatterns");
    	
    	assertEquals(1, parser.getPatterns().stream().filter(pat -> pat.getName().equals("logevents")).count(), "there should be pattern named 'logevents'");
    	assertEquals(1, parser.getPatterns().stream().filter(pat -> pat.getName().equals("logevents")).findFirst().get().getParents().size(), "pattern named 'logevents' should 1 parent");
    	assertEquals("events", parser.getPatterns().stream().filter(pat -> pat.getName().equals("logevents")).findFirst().get().getParents().get(0), "pattern named 'logevents' should have parent named 'events'");
    }
    
    @Test
    public void testBuilder() throws Exception {
    	Parser parser;
    	PatternDefinition intPattern = new PatternDefinition.Builder("custom_int").withParent("events").withPattern("%{INT:customvalue}").build();
    	
    	PatternDefinition strPattern = new PatternDefinition.Builder("custom_str").withParent("events").withPattern("%{LD:customvalue}").build();
    	PatternDefinition substrPattern = new PatternDefinition.Builder("custom_key").withParent("custom_str").withSourceField("customvalue").withPattern("key=%{LD:keyvalue}").build();
    	
    	parser = new Parser.Builder().withPatternDefinition(intPattern).withPatternDefinition(strPattern).withPatternDefinition(substrPattern).build();
    	assertEquals(5, parser.getPatterns().size(), "there should be 5 patterns");
    	
    	Map<String, Object> data;
    	data = parser.matchline("4");
    	assertEquals(4, data.get("customvalue"), "parsed result should have field 'customvalue' with value 4");
    	
    	data = parser.matchline("random string");
    	assertEquals("random string", data.get("customvalue"), "parsed result should have field 'customvalue' with value 'random string'");

    	data = parser.matchline("key=keyval");
    	assertTrue(((List<String>)data.get("__match")).contains("custom_str"), "parsed event should have custom_str as a parent eventType");
    	assertEquals("custom_key", data.get("eventType"), "parsed event should be of type custom_key");
    	assertEquals("keyval", data.get("keyvalue"), "parsed result should have field 'keyvalue' with value 'keyval'");
    }

    
    @Test
    public void testSrctimeParser() throws Exception {
    	Parser parser;
    	PatternDefinition pattern = new PatternDefinition.Builder("with_srctime").withParent("events").withPattern("%{TIMESTAMP_ISO8601:srctime}\t%{LD:data}").build();
    	pattern.put("srctime-field", "srctime");
    	pattern.put("srctime-format", "yyyy-MM-dd'T'hh:mm:ssX");
    	
    	parser = new Parser.Builder().withPatternDefinition(pattern).build();
    			
    	assertEquals(3, parser.getPatterns().size(), "there should be 3 patterns");

    	Map<String, Object> data;
    	data = parser.matchline("2019-03-07T00:00:14+02:00\tmydata");
    	
    	assertEquals(1551909614000L, data.get("src_logts_timestamp"), "srctime does not match");
    	assertEquals("mydata", data.get("data"), "data does not match");
    }
    
    @Test
    public void testBasePatterns() throws Exception {

    	Parser parser = new Parser();
    	parser = new Parser.Builder().build();
    	
    	Map<String, Object> data;
    	
    	
    	data = parser.matchline("logline");
    	assertTrue(data.containsKey("eventType"), "there should be an event type");
    	assertTrue(data.containsKey("data"), "there should be an event type");
    	assertEquals("events", data.get("eventType"), "event type should be 'events'");
    	assertEquals("logline", data.get("data"), "wrong parse output");
    	
    	data = parser.matchline("2020-04-14T14:11:21+03:00\tlocalhost\t1234\tlogline");
    	assertTrue(data.containsKey("eventType"), "there should be an event type");
    	assertEquals("logevents", ((String)data.get("eventType")), "eventType should be 'logevents'");
    	assertTrue(data.containsKey("data"), "there should be an data");
    	assertTrue(data.containsKey("__match"), "there should be __matches filed");
    	assertTrue(data.get("__match") instanceof List, "__matches filed should be List");
    	assertEquals(2, (((List)data.get("__match")).size()), "there should be 2 __matches");
    	assertEquals("localhost", ((String)data.get("host")), "host should be 'localhost'");
    	assertEquals("1234", ((String)data.get("pid")), "pid should be '1234'");
    	assertEquals("logline", ((String)data.get("data")), "data should be 'logline'");
    	
    }
    
}
