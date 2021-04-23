package com.github.arikastarvo.comet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.github.arikastarvo.comet.parser.Parser;
import com.github.arikastarvo.comet.parser.PatternDefinition;

public class ParserTest {
    
    @Test
    public void testBaseInit() throws Exception {

    	Parser parser = new Parser();
    	assertEquals("there should be 2 basepatterns", 2, parser.getPatterns().size());
    	
    	parser = new Parser.Builder().build();
    	assertEquals("there should be 2 basepatterns", 2, parser.getPatterns().size());
    	
    	assertEquals("there should be pattern named 'logevents'", 1, parser.getPatterns().stream().filter(pat -> pat.getName().equals("logevents")).count());
    	assertEquals("pattern named 'logevents' should 1 parent", 1, parser.getPatterns().stream().filter(pat -> pat.getName().equals("logevents")).findFirst().get().getParents().size());
    	assertEquals("pattern named 'logevents' should have parent named 'events'", "events", parser.getPatterns().stream().filter(pat -> pat.getName().equals("logevents")).findFirst().get().getParents().get(0));
    }
    
    @Test
    public void testBuilder() throws Exception {
    	Parser parser;
    	PatternDefinition intPattern = new PatternDefinition.Builder("custom_int").withParent("events").withPattern("%{INT:customvalue}").build();
    	
    	PatternDefinition strPattern = new PatternDefinition.Builder("custom_str").withParent("events").withPattern("%{LD:customvalue}").build();
    	PatternDefinition substrPattern = new PatternDefinition.Builder("custom_key").withParent("custom_str").withSourceField("customvalue").withPattern("key=%{LD:keyvalue}").build();
    	
    	parser = new Parser.Builder().withPatternDefinition(intPattern).withPatternDefinition(strPattern).withPatternDefinition(substrPattern).build();
    	assertEquals("there should be 5 patterns", 5, parser.getPatterns().size());
    	
    	Map<String, Object> data;
    	data = parser.matchline("4");
    	assertEquals("parsed result should have field 'customvalue' with value 4", 4, data.get("customvalue"));
    	
    	data = parser.matchline("random string");
    	assertEquals("parsed result should have field 'customvalue' with value 'random string'", "random string", data.get("customvalue"));

    	data = parser.matchline("key=keyval");
    	assertTrue("parsed event should have custom_str as a parent eventType", ((List<String>)data.get("__match")).contains("custom_str"));
    	assertEquals("parsed event should be of type custom_key", "custom_key", data.get("eventType"));
    	assertEquals("parsed result should have field 'keyvalue' with value 'keyval'", "keyval", data.get("keyvalue"));
    }

    
    @Test
    public void testSrctimeParser() throws Exception {
    	Parser parser;
    	PatternDefinition pattern = new PatternDefinition.Builder("with_srctime").withParent("events").withPattern("%{TIMESTAMP_ISO8601:srctime}\t%{LD:data}").build();
    	pattern.put("srctime-field", "srctime");
    	pattern.put("srctime-format", "yyyy-MM-dd'T'hh:mm:ssX");
    	
    	parser = new Parser.Builder().withPatternDefinition(pattern).build();
    			
    	assertEquals("there should be 3 patterns", 3, parser.getPatterns().size());

    	Map<String, Object> data;
    	data = parser.matchline("2019-03-07T00:00:14+02:00\tmydata");
    	
    	assertEquals("srctime does not match", 1551909614000L, data.get("src_logts_timestamp"));
    	assertEquals("data does not match", "mydata", data.get("data"));
    }
    
    @Test
    public void testBasePatterns() throws Exception {

    	Parser parser = new Parser();
    	parser = new Parser.Builder().build();
    	
    	Map<String, Object> data;
    	
    	
    	data = parser.matchline("logline");
    	assertTrue("there should be an event type", data.containsKey("eventType"));
    	assertTrue("there should be an event type", data.containsKey("data"));
    	assertEquals("event type should be 'events'", "events", data.get("eventType"));
    	assertEquals("wrong parse output", "logline", data.get("data"));
    	
    	data = parser.matchline("2020-04-14T14:11:21+03:00\tlocalhost\t1234\tlogline");
    	assertTrue("there should be an event type", data.containsKey("eventType"));
    	assertEquals("eventType should be 'logevents'", "logevents", ((String)data.get("eventType")));
    	assertTrue("there should be an data", data.containsKey("data"));
    	assertTrue("there should be __matches filed", data.containsKey("__match"));
    	assertTrue("__matches filed should be List", data.get("__match") instanceof List);
    	assertEquals("there should be 2 __matches", 2, (((List)data.get("__match")).size()));
    	assertEquals("host should be 'localhost'", "localhost", ((String)data.get("host")));
    	assertEquals("pid should be '1234'", "1234", ((String)data.get("pid")));
    	assertEquals("data should be 'logline'", "logline", ((String)data.get("data")));
    	
    }
    
}
