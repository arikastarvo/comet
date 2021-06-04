package com.github.arikastarvo.comet.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.csv.CSVInput;
import com.github.arikastarvo.comet.input.csv.CSVInputConfiguration;

public class CSVInputTest {

    @Test
    public void CSVInputConfigurationTest() throws Exception {
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		CSVInputConfiguration inputConfiguration = new CSVInputConfiguration(runtimeConfiguration);
		

		assertTrue(inputConfiguration.getSupportedSchemeList().contains("csv"));
		assertTrue(inputConfiguration.isSchemeSupported("csv"));
		assertFalse(inputConfiguration.isSchemeSupported("json"));

		Exception exception;

		exception = assertThrows(InputDefinitionException.class, () -> {
			inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>());
        });
		assertEquals(exception.getMessage(), "for CSV input, file or content field must be set");
		
		exception = assertThrows(InputDefinitionException.class, () -> {
			inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
				put("file", "csv-file.csv");
			}});
        });
		assertEquals(exception.getMessage(), "for CSV input, name must be set");

		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("content", "value1,value2,value3");
			put("name", "csv-input");
			put("fields", new HashMap<String, String>() {{
				put("field1", "string");
				put("field2", "string");
				put("field3", "string");
			}});
			put("header", false);
		}});
		assertEquals(inputConfiguration.name, "csv-input");
		assertEquals(inputConfiguration.content, "value1,value2,value3");
	
		CSVInput input = inputConfiguration.createInputInstance();
		assertEquals(input.getClass(), CSVInput.class);
	}
	
    @Test
    public void CSVInputConfigurationURITest() throws Exception {
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		CSVInputConfiguration inputConfiguration = new CSVInputConfiguration(runtimeConfiguration);
		
		Map<String, Object> conf;
		conf = inputConfiguration.parseURIInputDefinition(new URI("csv-file.csv"));
		assertEquals(conf.get("name"), "csv-file.csv");
		assertTrue(((List<String>)conf.get("file")).contains("csv-file.csv"));
	}
		
    @Test
    public void CSVInputTest() throws Exception {
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		CSVInputConfiguration inputConfiguration = new CSVInputConfiguration(runtimeConfiguration);
		
		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("content", "value1,value2,value3");
			put("name", "csv-input");
			put("fields", new HashMap<String, String>() {{
				put("field1", "string");
				put("field2", "string");
				put("field3", "string");
			}});
			put("header", false);
			put("window", false);
		}});
		
		CSVInput input = inputConfiguration.createInputInstance();
		input.init(runtimeConfiguration);
		assertEquals(1, runtimeConfiguration.getEventTypes().stream().filter( (Map<String, Object> evt) -> evt.get("name").equals(input.id)).count());
		Map<String, Object> runtimeEvent = runtimeConfiguration.getEventTypes().stream().filter( (Map<String, Object> evt) -> evt.get("name").equals(input.id)).findAny().get();
		assertTrue(((Map<String, String>)runtimeEvent.get("fields")).containsKey("field1"));
	}
}
