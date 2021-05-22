package com.github.arikastarvo.comet;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.arikastarvo.comet.input.file.FileInput;
import com.github.arikastarvo.comet.input.file.FileInputConfiguration;
import com.github.arikastarvo.comet.input.stdin.StdinInput;
import com.github.arikastarvo.comet.input.stdin.StdinInputConfiguration;
import com.github.arikastarvo.comet.output.file.FileOutput;
import com.github.arikastarvo.comet.output.file.FileOutputConfiguration;
import com.github.arikastarvo.comet.output.stdout.StdoutOutput;
import com.github.arikastarvo.comet.output.stdout.StdoutOutputConfiguration;

import net.sourceforge.argparse4j.inf.Namespace;

public class CometConfigurationCmdlineArg {
	
	static Logger log = LogManager.getLogger(CometConfigurationCmdlineArg.class);
	static CometApplication app;

	public static void parseApplicationConfiguration(Namespace pargs, CometApplication app, CometApplicationConfiguration conf) {
		
		if(pargs.getString("log_path") != null) {
			conf.logConfiguration.logPath = pargs.getString("log_path");
		}
		
		if(pargs.getBoolean("daemonize")) {
			conf.daemonize = true;
		}

		if (pargs.getString("data_path") != null && pargs.getString("data_path").trim().length() > 0) {
			log.debug("setting default global data path to '{}' ", pargs.getString("data_path"));
			conf.defaultDataPath = pargs.getString("data_path");
		}

		if(pargs.getList("profile") != null) {
			conf.profiles = pargs.getList("profile");
		}
	}
	
	public static void parseRuntimeConfiguration(Namespace pargs, CometApplication appObj, MonitorRuntimeConfiguration conf) {

		app = appObj;
		
		if(pargs.getList("p") != null) {
			pargs.getList("p").forEach((Object p) -> {
				File potentialPatternReference = new File(appObj.configuration.basepath + File.separator + (String)p);
				if(potentialPatternReference.exists()) {
					conf.addPatternReference(potentialPatternReference.getAbsolutePath());
				} else {
					conf.addPattern(MonitorRuntimeConfiguration.createPatternFromExpression((String)p));
				}
				
			});
		}
		
		if(pargs.getString("secrets") != null && new File((String)pargs.getString("secrets")).exists()) {
			conf.secretsPath = pargs.getString("secrets");
		}
		
		if(pargs.getList("p") != null && pargs.getList("p").size() > 0) {
			conf.usePatternset = false;
		}
		if(pargs.getBoolean("no_patternset") != null && pargs.getBoolean("no_patternset")) {
			conf.usePatternset = false;
		}

		if(pargs.getList("e") != null && pargs.getList("e").size() > 0) {
			conf.addEventTypesToParse(pargs.getList("e"));
		}

		if(pargs.getString("n") != null) {
			conf.runtimeName = pargs.getString("n");
		}
		
		if(pargs.getBoolean("k") == true) {
			conf.keepMatches = true;
		}

		if(pargs.getBoolean("remove_raw_data")) {
			conf.removeRawData = true;
		}

		if(pargs.getList("query") != null && pargs.getList("query").size() > 0) {
			Integer max = 0;
			try {
				max = conf.getStatements().keySet().stream().filter( (String id) -> id.matches("^command-line-[0-9]+$")).map( (String id) -> Integer.parseInt(id.substring(14))).max(Integer::compare).get();
			} catch(NoSuchElementException e) {
				// pass
			}
			//conf.addStatement("command-line-" + String.valueOf(++max), String.join(";\n", pargs.getList("query")), null);
			for(Object query : pargs.getList("query")) {
				//conf.addStatement((String)query);
				conf.addStatement("command-line-" + String.valueOf(++max), (String)query, null);
			};
		}

		
		// set file output
		if(pargs.getString("out_file") != null && !pargs.getString("out_file").equals("-")) {
			String fileOutput = pargs.getString("out_file");
			/* handle special case of output filename */
			if(fileOutput.equals("file")) {
				fileOutput = conf.runtimeName + "-output.log";
			}
			
			String fileOutFormat = null;
			if(pargs.getString("out_format") != null) {
				fileOutFormat = pargs.getString("out_format");
			}

			FileOutputConfiguration oc = new FileOutputConfiguration(new File(fileOutput), fileOutFormat);
			try {
				conf.addListener(new EventUpdateListener(new FileOutput(oc)));
			} catch (FileNotFoundException e) {
				log.error("creating file output failed: " + e.getMessage());
				log.debug("creating file output failed: " + e.getMessage(), e);
			}
			
		}

		// set stdout output 
		if(pargs.getBoolean("stdout")) {
			conf.addListener(new EventUpdateListener(new StdoutOutput(new StdoutOutputConfiguration((String)pargs.getString("out_format")))));
		}
		
		// set output format in case no output is explicitly defined and stdout is to be used with output formatting
		if(pargs.getString("out_format") != null) {
			conf.outputFormat = pargs.getString("out_format");
		}
		
		/** 
		 *  HANDLE INPUT
		 */
		
		if(pargs.getBoolean("stdin")) {
			conf.addInput(new StdinInput(new StdinInputConfiguration(conf)));
		}
		
		// SET INPUT TEST FILES
		if(pargs.getBoolean("test")) {
			FileInputConfiguration fic = new FileInputConfiguration(conf);
			fic.files = Arrays.asList("src/test/resources/samples1.log", "src/test/resources/samples2.log");
			conf.addInput(new FileInput(fic));
		}

		if(pargs.getList("file") != null && pargs.getList("file").size() > 0) {
			log.debug("configuring file input with '{}'", pargs.getList("file"));
			boolean tail = false;
			if(pargs.getBoolean("tail")) {
				tail = true;
			}
			FileInputConfiguration fic = new FileInputConfiguration(conf);
			fic.files = pargs.getList("file");
			fic.tail = tail;
			conf.addInput(new FileInput(fic));
			if(!tail) {
				log.debug("setting external clocking to be true because of file input (can be overriden to internal clocking with configuration)");
				conf.externalClock = true;
			}
		}

		if(pargs.getList("repeat_file") != null && pargs.getList("repeat_file").size() > 0) {
			FileInputConfiguration fic = new FileInputConfiguration(conf);
			fic.files = pargs.getList("repeat_file");
			if(pargs.getInt("repeat_file_interval") != null) {
				fic.repeatInSeconds = pargs.getInt("repeat_file_interval");
			}
			conf.addInput(new FileInput(fic));
		}
		
		if(pargs.getString("clock") != null) {
			if(pargs.getString("clock").equals("internal")) {
				log.debug("setting internal clocking explicitly");
				conf.externalClock = false;
			} else if (pargs.getString("clock").equals("external")) {
				log.debug("setting external clocking explicitly");
				conf.externalClock = true;
			}
		}

	}
}
