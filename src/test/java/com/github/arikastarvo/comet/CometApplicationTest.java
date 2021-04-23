package com.github.arikastarvo.comet;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Map;

import org.junit.Test;

import com.github.arikastarvo.comet.MonitorRuntime;
import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.file.FileInput;
import com.github.arikastarvo.comet.input.file.FileInputConfiguration;
import com.github.arikastarvo.comet.output.NoopOutput;
import com.github.arikastarvo.comet.parser.Parser;
import com.github.arikastarvo.comet.runtime.MonitorRuntimeEsperImpl;

public class CometApplicationTest {
    
    @Test
    public void testOutputFormat() throws Exception {
 
    	
    	MonitorRuntimeConfiguration runtimeConf = new MonitorRuntimeConfiguration(null);
    	FileInputConfiguration ic = new FileInputConfiguration(runtimeConf);
    	ic.files = Arrays.asList("src/test/resources/one-logevent.log");
    	runtimeConf.addInput(new FileInput(ic));
    	MonitorRuntime runtime = new MonitorRuntimeEsperImpl(runtimeConf);

    	Parser parser = new Parser.Builder()
    			.withStats()
    			.build();
    	
    	runtime.addParser(MonitorRuntime.DEFAULT_PARSER_ID, parser);
    	
    	InMemoryStdOutput imo = new InMemoryStdOutput("Logline from ${host} @ ${logts}. Source PID was ${pid}.");
    	CountingUpdateListener cul = new CountingUpdateListener(imo);
    	
    	runtime.configuration.removeListeners();
    	runtime.configuration.addListener(cul);
    	
    	runtime.addStatement("select * from logevents where host = 'localhost'");
    	
    	runtime.waitInput = true;
    	runtime.run();
		
    	assertEquals("total eventcount wrong", 1, cul.totalNewEvents);
    	
    	Map<String, Map<String, Integer>> stats = runtime.getParser().getStats().stats();
    	assertEquals("there should be at least one 'logevents'", 1, (int)stats.get("matchcount").get("logevents"));

    	String s = "Logline from localhost @ 2020-04-14T14:11:21+03:00. Source PID was 1234.";
    	assertEquals("memdata count wrong", 1, imo.memData.size());
    	assertEquals("memdata content wrong", s, imo.memData.get(0));
    	
    	runtime.Stop();
    }
}
