package com.github.arikastarvo.comet;

import org.junit.jupiter.api.Test;

public class CometApplicationCmdlineArgumentsTest {

	@Test
    public void testStartup() throws Exception {
    	CometApplication.main(new String[] {"--test"});
    }

	@Test
    public void testStartup2() throws Exception {
    	CometApplication.main(new String[]{"--test", "-p", "%{LD:data}", "select source,log from events"});
    }
}
