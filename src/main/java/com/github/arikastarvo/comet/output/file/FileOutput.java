package com.github.arikastarvo.comet.output.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.arikastarvo.comet.output.Output;
import com.github.arikastarvo.comet.output.OutputConnector;
import com.github.arikastarvo.comet.output.stdout.StdoutOutput;

import org.apache.commons.text.StringSubstitutor;

import com.jsoniter.output.JsonStream;

@OutputConnector(
	name = FileOutput.NAME,
	configuration = FileOutputConfiguration.class
)
public class FileOutput extends Output {

	public static final String NAME = "file";

	FileOutputConfiguration oc;
	PrintStream outputStream;

	public FileOutput(FileOutputConfiguration oc) throws FileNotFoundException {
		this.oc = oc;
		this.outputStream = new PrintStream(new FileOutputStream(oc.outFile));
		
		if(oc.template != null) {
			File file = new File(oc.template);
			if(file.exists()) {
				BufferedReader reader;
				try {
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			        oc.template = reader.lines().collect(Collectors.joining(System.lineSeparator()));
				} catch (FileNotFoundException e) {
					// pass this .. totally normal
				}
			}
		}
	}

	@Override
	public FileOutputConfiguration getOutputConfiguration() {
		return this.oc;
	}
	
	public void printOutput(Map<String, Object> data) {
		if(oc.template != null) {
			printOutput(new StringSubstitutor(StdoutOutput.asFlattendMap(data)).replace(oc.template));
		} else {
			printOutput((Object)data);
		}
	}
	
	public void printOutput(Object data) {
		if(oc.template != null) {
			printOutput(new StringSubstitutor(StdoutOutput.asFlattendMap((Map<String, Object>)data)).replace(oc.template));
		} else {
			printOutput(JsonStream.serialize(data));
		}
	}
	
	public void printOutput(String data) {
		this.outputStream.println(data);
		this.outputStream.flush();
	}

	public void Stop() {
		this.outputStream.flush();
		this.outputStream.close();
	}
	
	public String getDescription() {
		return "File -> " + oc.outFile.getName();
	}
}
