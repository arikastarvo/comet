package com.github.arikastarvo.comet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Map;

import com.github.arikastarvo.comet.input.file.FileInput;
import com.github.arikastarvo.comet.input.file.FileInputConfiguration;
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
		
    	assertEquals(1, cul.totalNewEvents, "total eventcount wrong");
    	
    	Map<String, Map<String, Integer>> stats = runtime.getParser().getStats().stats();
    	assertEquals(1, (int)stats.get("matchcount").get("logevents"), "there should be at least one 'logevents'");

    	String s = "Logline from localhost @ 2020-04-14T14:11:21+03:00. Source PID was 1234.";
    	assertEquals(1, imo.memData.size(), "memdata count wrong");
    	assertEquals(s, imo.memData.get(0), "memdata content wrong");
    	
    	runtime.Stop();
    }
}
