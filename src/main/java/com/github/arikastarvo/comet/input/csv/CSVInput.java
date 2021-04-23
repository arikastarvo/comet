package com.github.arikastarvo.comet.input.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;

import com.github.arikastarvo.comet.InputEventReceiver;
import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.FiniteInput;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.InputConfiguration;
import com.github.arikastarvo.comet.input.ReferenceInput;
import com.github.arikastarvo.comet.reference.ReferenceReloadCallback;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class CSVInput extends Input<CSVInput> implements FiniteInput, ReferenceInput {

	CSVInputConfiguration ic;
	
	private CSVReader csvReader;

	
	private InputEventReceiver ier = null;
	private ReferenceReloadCallback callback;
	
	public CSVInput(CSVInputConfiguration ic) {
		this.log = LogManager.getLogger(CSVInput.class);
		this.ic  = ic;
	}

	@Override
	public CSVInputConfiguration getInputConfiguration() {
		return this.ic;
	}
	
	@Override
	public void shutdown() {
		log.debug("CSVInput '{}' shutting down", this.id);
	}
	
	public void init(MonitorRuntimeConfiguration monitorRuntimeConfiguration) {		
		super.init(monitorRuntimeConfiguration);

		Map<String,Object> mergedFields = new HashMap<String, Object>();
		
		// read first line (if header exixts)
		boolean headerParsed = false;
		if(ic.header) {
			
			BufferedReader reader = null;// new BufferedReader(new );
			try {
				if(ic.file != null && ic.file.length() > 0) {
					File fileObj = new File(ic.file);
					if(fileObj.exists() && fileObj.isFile()) {
						reader = new BufferedReader(new FileReader(new File(ic.file)));
					}
				} else if(ic.content != null && ic.content.length() > 0) {
					reader = new BufferedReader(new StringReader(ic.content));
				}
				
				if(reader == null) {
					throw new IllegalArgumentException("could not get any csv data for header");					
				}
				
				csvReader = new CSVReader(reader);
				String[] header = csvReader.readNext();
				for(String field : header) {
					mergedFields.put(field, "string");
				}
				headerParsed = true;
			    reader.close();
			    csvReader.close();
			} catch (CsvValidationException | IOException | IllegalArgumentException e) {
				log.error("could not parse fileds from csv header in csv input {}, error: {}", this.id, e.getMessage());
			}
		}
		if(headerParsed) {
			for(Map.Entry<String, String> entry : ic.fields.entrySet()) {
				if (mergedFields.containsKey(entry.getKey())) {
					mergedFields.put(entry.getKey(), entry.getValue());
				}
			}
		} else {
			mergedFields.putAll(ic.fields);
		}
		
		if(ic.createAsWindow) {
			
			String createFields = mergedFields.entrySet().stream().map((Entry<String, Object> entry) -> entry.getKey() + " " + (String)entry.getValue()).collect(Collectors.joining(", "));
			String publicEventType = String.format("@public @buseventtype create schema %s (%s);", id + "Event", createFields);
			String createQuery = String.format("@Name(\"%s\") @Tag(name=\"silent\", value=\"true\") @Public create window %s#keepall (%s);", id, id, createFields);
			String insertFields = mergedFields.entrySet().stream().map((Entry<String, Object> entry) -> entry.getKey()).collect(Collectors.joining(", "));
			String insertQuery = String.format("@Name(\"InsertInto%s\") @Tag(name=\"silent\", value=\"true\") insert into %s select %s from %s;", id, id, insertFields, id + "Event");

			monitorRuntimeConfiguration.addExtraStatement(publicEventType + " " + createQuery + " " + insertQuery);
			
		} else {
			// just create new event type
			monitorRuntimeConfiguration.addEventType(id, mergedFields);
		}
		
	}
	
	public void run() {

		if(callback != null) {
			callback.reloadInit();
		}
		
		if(ier == null) {
			ier = this.monitorRuntime;
		}
		
		try {
	
			BufferedReader reader = null;

			if(ic.file != null && ic.file.length() > 0) {
				File fileObj = new File(ic.file);
				if(fileObj.exists() && fileObj.isFile()) {
					reader = new BufferedReader(new FileReader(fileObj));
				}
			} else if(ic.content != null && ic.content.length() > 0) {
				reader = new BufferedReader(new StringReader(ic.content));
			}
			
			if(reader == null) {
				throw new IllegalArgumentException("could not get any csv data");					
			}
			
			csvReader = new CSVReader(reader);
			
			String[] header;
			
			if(ic.header) {
				header = csvReader.readNext();
			} else {
				header = ic.fields.keySet().toArray(new String[0]);
			}
			
			String[] line;
			while ((line = csvReader.readNext()) != null) {
				int i = 0;
				Map<String, Object> obj = new HashMap<String, Object>();
		    	for(String field : header) {
		    		Object value = line[i];
		    		if(ic.fields.containsKey(field)) {
		    			switch(ic.fields.get(field)) {
		    				case "int":
		    					value = Integer.parseInt(line[i]);
		    					break;
		    			}
		    		}
		    		obj.put(field, value);
		    		i++;
		    	}
		    	ier.send(id + (ic.createAsWindow?"Event":""), obj);
		    }
		    reader.close();
		    csvReader.close();			
			
		} catch (FileNotFoundException e) {
			log.error("could not open file {} in CSVInput {}, error: {}", ic.file, this.id, e.getMessage());
		} catch (IOException e) {
			log.error("could not read file {} in CSVInput {}, error: {}", ic.file, this.id, e.getMessage());
		} catch (CsvValidationException e) {
			log.error("could not parse csv in file {}, CSVInput {}, error: {}", ic.file, this.id, e.getMessage());
		}


		if(callback != null) {
			callback.reloadFinalize();
		}
		
		Stop();
	}

	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public boolean isFinite() {
		return true;
	}

	@Override
	public void setInputEventReceiver(InputEventReceiver ier) {
		this.ier = ier;
	}

	@Override
	public void setReloadCallback(ReferenceReloadCallback callback) {
		this.callback = callback;
	}

}
