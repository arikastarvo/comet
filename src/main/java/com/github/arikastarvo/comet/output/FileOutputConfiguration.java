package com.github.arikastarvo.comet.output;

import java.io.File;

public class FileOutputConfiguration {

	public String template;
	public File outFile;
	
	public FileOutputConfiguration() {}
	public FileOutputConfiguration(File file, String template) { this.outFile = file; this.template = template; }
}
