package com.github.arikastarvo.comet.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.arikastarvo.comet.CountingUpdateListener;
import com.github.arikastarvo.comet.InMemoryInputEventReceiver;
import com.github.arikastarvo.comet.InMemoryStdOutput;
import com.github.arikastarvo.comet.MonitorRuntime;
import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.file.FileInput;
import com.github.arikastarvo.comet.input.file.FileInputConfiguration;
import com.github.arikastarvo.comet.runtime.MonitorRuntimeEsperImpl;

public class FileInputTest {

    @Test
    public void FileInputConfigurationTest() throws Exception {
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		FileInputConfiguration inputConfiguration = new FileInputConfiguration(runtimeConfiguration);
		
		assertTrue(inputConfiguration.getSupportedSchemeList().contains("file"));
		assertTrue(inputConfiguration.isSchemeSupported("file"));
		assertFalse(inputConfiguration.isSchemeSupported("clouds"));

		Exception exception;

		exception = assertThrows(InputDefinitionException.class, () -> {
			inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>());
        });
		assertEquals(exception.getMessage(), "no type defined or wrong type for file input");

		
		exception = assertThrows(InputDefinitionException.class, () -> {
			inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
				put("type", "file");
			}});
        });
		assertEquals(exception.getMessage(), "file has to be specified for file input");

		exception = assertThrows(InputDefinitionException.class, () -> {
			inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
				put("type", "file");
				put("file", new File("filename.log"));
			}});
		});
		assertEquals(exception.getMessage(), "file has to be defined as a string or list of strings");

		assertEquals(inputConfiguration.tail, false);
		assertEquals(inputConfiguration.repeatInSeconds, 0);

		exception = assertThrows(InputDefinitionException.class, () -> {
			inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
				put("type", "file");
				put("file", "filename.log");
				put("tail", true);
				put("repeat", 1);
			}});
		});
		assertEquals(exception.getMessage(), "tail and repeat can not be defined together for file input");

		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("type", "file");
			put("name", "file-input");
			put("file", "filename.log");
			put("tail", true);
		}});
		assertEquals(inputConfiguration.name, "file-input");
		assertEquals(1, inputConfiguration.files.size());
		assertTrue(inputConfiguration.files.contains("filename.log"));
		assertTrue(inputConfiguration.tail);
		
		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("type", "file");
			put("file", Arrays.asList("filename.log", "filename2.log"));
		}});
		assertEquals(2, inputConfiguration.files.size());
		assertTrue(inputConfiguration.files.contains("filename.log"));
		assertTrue(inputConfiguration.files.contains("filename2.log"));

		FileInput input = inputConfiguration.createInputInstance();
		assertEquals(input.getClass(), FileInput.class);
	}
	
    @Test
    public void FileInputConfigurationURITest() throws Exception {
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		FileInputConfiguration inputConfiguration = new FileInputConfiguration(runtimeConfiguration);
		
		Map<String, Object> conf;

		conf = inputConfiguration.parseURIInputDefinition(new URI("filename.log"));
		assertTrue(((List<String>)conf.get("file")).contains("filename.log"));

		conf = inputConfiguration.parseURIInputDefinition(new URI("file:filename.log"));
		assertTrue(((List<String>)conf.get("file")).contains("filename.log"));

		conf = inputConfiguration.parseURIInputDefinition(new URI("file:/filename.log"));
		assertTrue(((List<String>)conf.get("file")).contains("/filename.log"));

		runtimeConfiguration.sourceConfigurationPath = "/tmp";
		conf = inputConfiguration.parseURIInputDefinition(new URI("file:filename.log"));
		assertTrue(((List<String>)conf.get("file")).contains("/tmp/filename.log"));
	}

    @Test
    public void FileInputTest_01() throws Exception {
		
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		runtimeConfiguration.sourceConfigurationPath = "src/test/resources/";
		FileInputConfiguration inputConfiguration = new FileInputConfiguration(runtimeConfiguration);

		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("type", "file");
			put("file", "one-logevent.log.gz");
		}});
		
		FileInput input = new FileInput(inputConfiguration);
		InMemoryInputEventReceiver inputReceiver = new InMemoryInputEventReceiver();
		input.setInputEventReceiver(inputReceiver);
		input.run();
		assertEquals(1, inputReceiver.data.size());
		Map<String, Object> lineObj = (Map<String, Object>)inputReceiver.data.get(0); 
		assertEquals("events", lineObj.get("type"));
		String data = "2020-04-14T14:11:21+03:00	localhost	1234	data";
		Map<String, Object> lineObjData = (Map<String, Object>)lineObj.get("data"); 
		assertEquals(data, lineObjData.get("data"));
	}
	
    @Test
    public void FileInputTest_02() throws Exception {
		
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		runtimeConfiguration.sourceConfigurationPath = "src/test/resources/";
		FileInputConfiguration inputConfiguration = new FileInputConfiguration(runtimeConfiguration);

		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("type", "file");
			put("file", "ten-logevent.log.gz");
		}});
		
		FileInput input = new FileInput(inputConfiguration);
		InMemoryInputEventReceiver inputReceiver = new InMemoryInputEventReceiver();
		input.setInputEventReceiver(inputReceiver);
		input.run();
		assertEquals(10, inputReceiver.data.size());

		Map<String, Object> lineObj = (Map<String, Object>)inputReceiver.data.get(0); 
		assertEquals("events", lineObj.get("type"));
		String data = "2020-04-14T14:11:21+03:00	localhost	1234	data";
		Map<String, Object> lineObjData = (Map<String, Object>)lineObj.get("data"); 
		assertEquals(data, lineObjData.get("data"));

		lineObj = (Map<String, Object>)inputReceiver.data.get(9); 
		assertEquals("events", lineObj.get("type"));
		lineObjData = (Map<String, Object>)lineObj.get("data"); 
		assertEquals(data, lineObjData.get("data"));

	}
}
