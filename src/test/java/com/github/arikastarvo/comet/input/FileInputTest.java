package com.github.arikastarvo.comet.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.file.FileInput;
import com.github.arikastarvo.comet.input.file.FileInputConfiguration;

public class FileInputTest {

    @Test
    public void FileInputConfigurationTest() throws Exception {
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		FileInputConfiguration inputConfiguration = new FileInputConfiguration(runtimeConfiguration);
		
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
    public void FileInputTest_01() throws Exception {
		
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		FileInputConfiguration inputConfiguration = new FileInputConfiguration(runtimeConfiguration);
		
		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("type", "file");
			put("file", "filename.log");
		}});
		FileInput input = new FileInput(inputConfiguration);
		assertEquals(input.getClass(), FileInput.class);
	}
}
