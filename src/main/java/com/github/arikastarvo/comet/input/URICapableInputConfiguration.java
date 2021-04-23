package com.github.arikastarvo.comet.input;

import java.net.URI;
import java.util.List;
import java.util.Map;

public interface URICapableInputConfiguration {
	
	public List<String> getSupportedSchemeList();
	
	default public boolean isSchemeSupported(String scheme) {
		return getSupportedSchemeList().contains(scheme);
	};
	public Map<String, Object> parseURIInputDefinition(URI inputDefinition);

}
