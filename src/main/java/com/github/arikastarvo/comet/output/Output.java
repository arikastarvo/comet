package com.github.arikastarvo.comet.output;

import java.util.Map;

public interface Output {

	public void printOutput(Map<String, Object> data);
	public void printOutput(Object data);	
	public void printOutput(String data);
	
	public void Stop();
	
	public String getDescription();
}