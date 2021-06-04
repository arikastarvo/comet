package com.github.arikastarvo.comet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Map;

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
    	assertEquals(true, runtimeConf.hasInputs(), "no inputs detected");
    	Map<String, Input> inputs = runtimeConf.getInputs();
    	
    	assertEquals(true, inputs.containsKey("noop"), "no noop found");
    	assertEquals(true, inputs.containsKey("stdin"), "no stdin found");
    	
    	assertEquals(true, inputs.containsKey("csvinput"), "no csvinput found");
    	assertEquals(true, ((CSVInput)inputs.get("csvinput")).getInputConfiguration().createAsWindow, "wrong window prop value for csvinput");
    	assertEquals(true, ((CSVInput)inputs.get("csvinput")).getInputConfiguration().content == null, "csvinput content field should be null");
    	
    	assertEquals(true, inputs.containsKey("csvinput2"), "no csvinput2 found");
    	assertEquals(true, ((CSVInput)inputs.get("csvinput2")).getInputConfiguration().content != null, "csvinput2 content field should be non-null");
    	
    	assertEquals(true, inputs.containsKey("listinput"), "no listinput found");
    	assertEquals(true, ((StaticListInput)inputs.get("listinput")).getInputConfiguration().createAsWindow, "wrong window prop value for listinput");
    	

    	assertEquals(true, inputs.containsKey("file_1"), "no file_1 found");
    	assertEquals(1, ((FileInput)inputs.get("file_1")).getInputConfiguration().files.size(), "there must be 1 file defined in file_1 input");
    	assertEquals(true, ((FileInput)inputs.get("file_1")).getInputConfiguration().files.get(0).endsWith("input-1.yaml"), "there must be input-1.yaml file defined in file_1 input");
    	
    }
    

    @Test
    public void Query_1() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/query-1.yaml", null, runtimeConf);
    	
    	// check for noop input
    	assertEquals(true, runtimeConf.hasInputs(), "no inputs detected");
    	Map<String, Input> inputs = runtimeConf.getInputs();
    	assertEquals(true, inputs.containsKey("noop"), "noop input not found");
    	
    	// statement
    	assertEquals(true, runtimeConf.hasStatements(), "no statements detected");
    	Map<String, Map<String, String>> statements = runtimeConf.getStatements();
    	assertEquals(1, statements.size(), "there must be one module deployed");
    	assertEquals("select * from events", statements.values().stream().findFirst().get().get("statement"), "wrong statement");
    }

    @Test
    public void Query_2() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/query-2.yaml", null, runtimeConf);
    	
    	// statement
    	assertEquals(true, runtimeConf.hasStatements(), "no statements detected");
    	Map<String, Map<String, String>> statements = runtimeConf.getStatements();
    	assertEquals(3, statements.size(), "there must be one module deployed");
    	assertEquals("select * from events", new ArrayList<Map<String, String>>(statements.values()).get(0).get("statement"), "wrong statement");
    	assertEquals("select * from logevents", new ArrayList<Map<String, String>>(statements.values()).get(1).get("statement"), "wrong statement");
    	assertEquals("select data from events", new ArrayList<Map<String, String>>(statements.values()).get(2).get("statement"), "wrong statement");
    }

    @Test
    public void Query_3() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/query-3.yaml", null, runtimeConf);
    	
    	// statement
    	assertEquals(true, runtimeConf.hasStatements(), "no statements detected");
    	Map<String, Map<String, String>> statements = runtimeConf.getStatements();
    	assertEquals(1, statements.size(), "there must be one module deployed");
    	assertEquals(true, statements.containsKey("myquery"), "there must be statement named 'myquery'");
    	assertEquals("select * from events", new ArrayList<Map<String, String>>(statements.values()).get(0).get("statement"), "wrong statement");
    }

    @Test
    public void Query_4() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/query-4.yaml", null, runtimeConf);
    	
    	// statement
    	assertEquals(true, runtimeConf.hasStatements(), "no statements detected");
    	Map<String, Map<String, String>> statements = runtimeConf.getStatements();
    	assertEquals(3, statements.size(), "there must be one module deployed");
    	assertEquals(true, statements.containsKey("myquery"), "there must be statement named 'myquery'");
    	assertEquals("select * from events", statements.get("myquery").get("statement"), "wrong statement");
    	assertEquals(true, statements.containsKey("myquery-1"), "there must be statement named 'myquery-1'");
    	assertEquals("select * from logevents", statements.get("myquery-1").get("statement"), "wrong statement");
    	assertEquals(true, statements.containsKey("myquery-2"), "there must be statement named 'myquery-2'");
    	assertEquals("select data from events", statements.get("myquery-2").get("statement"), "wrong statement");
    }


    @Test
    public void Persistence_1() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/persistence-1.yaml", null, runtimeConf);
    	
    	// tests
    	assertEquals(true, runtimeConf.persistenceConfiguration.initialLoad, "initial load should be true by default");
    	assertEquals(60, runtimeConf.persistenceConfiguration.persistenceInterval, "initial persistence interval should be 60");
    	assertEquals(1, runtimeConf.persistenceConfiguration.persistence.size(), "there must be 1 persistence item");
    	assertEquals("eventsWindow", runtimeConf.persistenceConfiguration.persistence.get(0), "persistence item must be eventsWindow");
    }

    @Test
    public void Persistence_2() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/persistence-2.yaml", null, runtimeConf);
    	
    	// tests
    	assertEquals(2, runtimeConf.persistenceConfiguration.persistence.size(), "there must be 2 persistence items");
    	assertTrue(runtimeConf.persistenceConfiguration.persistence.contains("eventsWindow"), "persistence item eventsWindow must exist");
    	assertTrue(runtimeConf.persistenceConfiguration.persistence.contains("logeventsWindow"), "persistence item logeventsWindow must exist");
    }

    @Test
    public void Persistence_3() throws Exception {
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	CometConfigurationYaml.parseConfiguration("src/test/resources/configs/persistence-3.yaml", null, runtimeConf);
    	
    	// tests
    	assertEquals(1, runtimeConf.persistenceConfiguration.persistence.size(), "there must be 1 persistence items");
    	assertEquals(10, runtimeConf.persistenceConfiguration.persistenceInterval, "persistence interval should be 10");
    }
    /** there should be some more different scenarios covered actually **/
}
