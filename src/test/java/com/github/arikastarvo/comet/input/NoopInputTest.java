package com.github.arikastarvo.comet.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.noop.NoopInput;
import com.github.arikastarvo.comet.input.noop.NoopInputConfiguration;

public class NoopInputTest {

    @Test
    public void NoopInputConfigurationTest() throws Exception {
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		NoopInputConfiguration inputConfiguration = new NoopInputConfiguration(runtimeConfiguration);
		
		Exception exception;

		exception = assertThrows(InputDefinitionException.class, () -> {
			inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>());
        });
		assertEquals(exception.getMessage(), "no type defined");

		
		exception = assertThrows(InputDefinitionException.class, () -> {
			inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
				put("type", "non-noop");
			}});
        });
		assertEquals(exception.getMessage(), String.format("type has to be %s", inputConfiguration.getInputType()));
		
		
		assertEquals(inputConfiguration.finite, false);

		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("type", "noop");
			put("name", "noop-input");
			put("finite", true);
		}});
		assertEquals(inputConfiguration.name, "noop-input");
		assertEquals(inputConfiguration.finite, true);
	
		NoopInput input = inputConfiguration.createInputInstance();
		assertEquals(input.getClass(), NoopInput.class);
	}
	
    @Test
    public void NoopInputTest_01() throws Exception {
		
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		NoopInputConfiguration inputConfiguration = new NoopInputConfiguration(runtimeConfiguration);
		
		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("type", "noop");
			put("name", "noop-input");
			put("finite", true);
		}});
		NoopInput input = new NoopInput(inputConfiguration);

		assertTrue(input.isFinite());

		inputConfiguration.finite = false;
		assertEquals(input.isAlive(), false);
		input.start();
		assertTrue(input.isAlive());
		input.shutdown();
	}
}
