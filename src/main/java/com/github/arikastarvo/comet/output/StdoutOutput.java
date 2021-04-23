package com.github.arikastarvo.comet.output;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.text.StringSubstitutor;

import com.jsoniter.output.JsonStream;

public class StdoutOutput implements Output {

	public static Map<String, Object> asFlattendMap(Map<String, Object> map) {
		return map.entrySet().stream()
				.flatMap(StdoutOutput::flatten)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	public static Stream<Map.Entry<String, Object>> flatten(Map.Entry<String, Object> entry) {
		if (entry.getValue() instanceof Map<?, ?>) {
			Map<String, Object> nested = (Map<String, Object>) entry.getValue();

			return nested.entrySet().stream()
					.map(e -> new AbstractMap.SimpleEntry(entry.getKey() + "." + e.getKey(), e.getValue()))
					.flatMap(StdoutOutput::flatten);
		}
		return Stream.of(entry);
	}

	String template;

	public StdoutOutput(StdoutOutputConfiguration oc) {
		this.template = oc.template;
		
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
		if(template != null) {
			printOutput(new StringSubstitutor(asFlattendMap((Map<String, Object>)data)).replace(template));
		} else {
			printOutput((Object)data);
		}
	}
	
	public void printOutput(Object data) {
		if(template != null) {
			printOutput(new StringSubstitutor(asFlattendMap((Map<String, Object>)data)).replace(template));
		} else {
			printOutput(JsonStream.serialize(data));
		}
	}
	
	public void printOutput(String data) {
		System.out.println(data);
	}

	public void Stop() {
		// pass
	}
	
	public String getDescription() {
		return "Stdout";
	}
}
