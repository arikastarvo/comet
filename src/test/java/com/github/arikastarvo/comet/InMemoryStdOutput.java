package com.github.arikastarvo.comet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;

import com.github.arikastarvo.comet.output.Output;
import com.github.arikastarvo.comet.output.OutputConfiguration;
import com.jsoniter.output.JsonStream;

public class InMemoryStdOutput extends Output {

	String template;
	List<Object> memData = new ArrayList<Object>();
	Boolean raw = false; 

	public InMemoryStdOutput() {
	}
	
	public InMemoryStdOutput(boolean raw) {
		this.raw = raw;
	}
	
	public InMemoryStdOutput(String template) {
		if(template != null) {
			File file = new File(template);
			if(file.exists()) {
				BufferedReader reader;
				try {
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			        this.template = reader.lines().collect(Collectors.joining(System.lineSeparator()));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				this.template = template;
			}
		}
	}
	
	public void printOutput(Map<String, Object> data) {
		if(raw) {
			memData.add(data);
		} else if(template != null) {
			printOutput(new StringSubstitutor(data).replace(template));
		} else {
			printOutput((Object)data);
		}
	}
	
	public void printOutput(Object data) {
		if(raw) {
			memData.add(data);
		} else if(template != null) {
			printOutput(new StringSubstitutor((Map<String, Object>)data).replace(template));
		} else {
			printOutput(JsonStream.serialize(data));
		}
	}
	
	public void printOutput(String data) {
		this.memData.add(data);
	}

	public void Stop() {
		// pass
	}

	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends OutputConfiguration<T>> OutputConfiguration<T> getOutputConfiguration() {
		return null;
	}
}