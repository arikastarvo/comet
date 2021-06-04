package com.github.arikastarvo.comet.input;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.noop.NoopInput;
import com.github.arikastarvo.comet.input.noop.NoopInputConfiguration;

public class NoopInputTest {
    
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void NoopInputConfigurationTest() throws Exception {
		MonitorRuntimeConfiguration runtimeConfiguration = new MonitorRuntimeConfiguration();
		NoopInputConfiguration inputConfiguration = new NoopInputConfiguration(runtimeConfiguration);
		
		exceptionRule.expect(InputDefinitionException.class);
		exceptionRule.expectMessage("no type defined");
		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>());

		exceptionRule.expectMessage(String.format("type has to be %s", inputConfiguration.getInputType()));
		inputConfiguration.parseMapInputDefinition(new HashMap<String, Object>(){{
			put("type", "non-noop");
		}});
		exceptionRule.expectMessage("");
		
		
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

		assertEquals(input.isFinite(), true);

		inputConfiguration.finite = false;
		assertEquals(input.isAlive(), false);
		input.start();
		assertEquals(input.isAlive(), true);
		input.shutdown();
	}
}
