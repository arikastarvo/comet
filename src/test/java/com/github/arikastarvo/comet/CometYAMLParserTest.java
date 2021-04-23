package com.github.arikastarvo.comet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Map;

import org.junit.Test;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.CometConfigurationYaml;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.csv.CSVInput;
import com.github.arikastarvo.comet.input.file.FileInput;
import com.github.arikastarvo.comet.input.list.StaticListInput;

public class CometYAMLParserTest {

    @Test
    public void Input_1() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/input-1.yaml", null, runtimeConf);
    	
    	// inputs
    	assertEquals("no inputs detected", true, runtimeConf.hasInputs());
    	Map<String, Input> inputs = runtimeConf.getInputs();
    	
    	assertEquals("no noop found", true, inputs.containsKey("noop"));
    	assertEquals("no stdin found", true, inputs.containsKey("stdin"));
    	
    	assertEquals("no csvinput found", true, inputs.containsKey("csvinput"));
    	assertEquals("wrong window prop value for csvinput", true, ((CSVInput)inputs.get("csvinput")).getInputConfiguration().createAsWindow);
    	assertEquals("csvinput content field should be null", true, ((CSVInput)inputs.get("csvinput")).getInputConfiguration().content == null);
    	
    	assertEquals("no csvinput2 found", true, inputs.containsKey("csvinput2"));
    	assertEquals("csvinput2 content field should be non-null", true, ((CSVInput)inputs.get("csvinput2")).getInputConfiguration().content != null);
    	
    	assertEquals("no listinput found", true, inputs.containsKey("listinput"));
    	assertEquals("wrong window prop value for listinput", true, ((StaticListInput)inputs.get("listinput")).getInputConfiguration().createAsWindow);
    	

    	assertEquals("no file_1 found", true, inputs.containsKey("file_1"));
    	assertEquals("there must be 1 file defined in file_1 input", 1, ((FileInput)inputs.get("file_1")).getInputConfiguration().files.size());
    	assertEquals("there must be input-1.yaml file defined in file_1 input", true, ((FileInput)inputs.get("file_1")).getInputConfiguration().files.get(0).endsWith("input-1.yaml"));
    	
    }
    

    @Test
    public void Query_1() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/query-1.yaml", null, runtimeConf);
    	
    	// check for noop input
    	assertEquals("no inputs detected", true, runtimeConf.hasInputs());
    	Map<String, Input> inputs = runtimeConf.getInputs();
    	assertEquals("noop input not found", true, inputs.containsKey("noop"));
    	
    	// statement
    	assertEquals("no statements detected", true, runtimeConf.hasStatements());
    	Map<String, Map<String, String>> statements = runtimeConf.getStatements();
    	assertEquals("there must be one module deployed", 1, statements.size());
    	assertEquals("wrong statement", "select * from events", statements.values().stream().findFirst().get().get("statement"));
    }

    @Test
    public void Query_2() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/query-2.yaml", null, runtimeConf);
    	
    	// statement
    	assertEquals("no statements detected", true, runtimeConf.hasStatements());
    	Map<String, Map<String, String>> statements = runtimeConf.getStatements();
    	assertEquals("there must be one module deployed", 3, statements.size());
    	assertEquals("wrong statement", "select * from events", new ArrayList<Map<String, String>>(statements.values()).get(0).get("statement"));
    	assertEquals("wrong statement", "select * from logevents", new ArrayList<Map<String, String>>(statements.values()).get(1).get("statement"));
    	assertEquals("wrong statement", "select data from events", new ArrayList<Map<String, String>>(statements.values()).get(2).get("statement"));
    }

    @Test
    public void Query_3() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/query-3.yaml", null, runtimeConf);
    	
    	// statement
    	assertEquals("no statements detected", true, runtimeConf.hasStatements());
    	Map<String, Map<String, String>> statements = runtimeConf.getStatements();
    	assertEquals("there must be one module deployed", 1, statements.size());
    	assertEquals("there must be statement named 'myquery'", true, statements.containsKey("myquery"));
    	assertEquals("wrong statement", "select * from events", new ArrayList<Map<String, String>>(statements.values()).get(0).get("statement"));
    }

    @Test
    public void Query_4() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/query-4.yaml", null, runtimeConf);
    	
    	// statement
    	assertEquals("no statements detected", true, runtimeConf.hasStatements());
    	Map<String, Map<String, String>> statements = runtimeConf.getStatements();
    	assertEquals("there must be one module deployed", 3, statements.size());
    	assertEquals("there must be statement named 'myquery'", true, statements.containsKey("myquery"));
    	assertEquals("wrong statement", "select * from events", statements.get("myquery").get("statement"));
    	assertEquals("there must be statement named 'myquery-1'", true, statements.containsKey("myquery-1"));
    	assertEquals("wrong statement", "select * from logevents", statements.get("myquery-1").get("statement"));
    	assertEquals("there must be statement named 'myquery-2'", true, statements.containsKey("myquery-2"));
    	assertEquals("wrong statement", "select data from events", statements.get("myquery-2").get("statement"));
    }


    @Test
    public void Persistence_1() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/persistence-1.yaml", null, runtimeConf);
    	
    	System.out.println(runtimeConf.persistenceConfiguration.persistence);
    	// tests
    	assertEquals("initial load should be true by default", true, runtimeConf.persistenceConfiguration.initialLoad);
    	assertEquals("initial persistence interval should be 60", 60, runtimeConf.persistenceConfiguration.persistenceInterval);
    	assertEquals("there must be 1 persistence item", 1, runtimeConf.persistenceConfiguration.persistence.size());
    	assertEquals("persistence item must be eventsWindow", "eventsWindow", runtimeConf.persistenceConfiguration.persistence.get(0));
    }

    @Test
    public void Persistence_2() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/persistence-2.yaml", null, runtimeConf);
    	
    	// tests
    	assertEquals("there must be 2 persistence items", 2, runtimeConf.persistenceConfiguration.persistence.size());
    	assertTrue("persistence item eventsWindow must exist", runtimeConf.persistenceConfiguration.persistence.contains("eventsWindow"));
    	assertTrue("persistence item logeventsWindow must exist", runtimeConf.persistenceConfiguration.persistence.contains("logeventsWindow"));
    }

    @Test
    public void Persistence_3() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/persistence-3.yaml", null, runtimeConf);
    	
    	// tests
    	assertEquals("there must be 1 persistence items", 1, runtimeConf.persistenceConfiguration.persistence.size());
    	assertEquals("persistence interval should be 10", 10, runtimeConf.persistenceConfiguration.persistenceInterval);
    }
    /** there should be some more different scenarios covered actually **/
}
