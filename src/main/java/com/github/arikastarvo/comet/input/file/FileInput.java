package com.github.arikastarvo.comet.input.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.logging.log4j.LogManager;

import com.github.arikastarvo.comet.InputEventReceiver;
import com.github.arikastarvo.comet.input.FiniteInput;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.InputConfiguration;
import com.github.arikastarvo.comet.input.InputConnector;
import com.github.arikastarvo.comet.input.ReferenceInput;
import com.github.arikastarvo.comet.input.RepeatableInput;
import com.github.arikastarvo.comet.reference.ReferenceReloadCallback;

@InputConnector(
	name = FileInput.NAME,
	configuration = FileInputConfiguration.class
)
public class FileInput extends Input<FileInput> implements RepeatableInput, ReferenceInput, FiniteInput {

	public static final String NAME = "file";

	FileInputConfiguration ic;
	ScheduledExecutorService executorService;
	
	ReferenceReloadCallback callback;
	
	List<Tailer> tailers = new ArrayList<Tailer>();
	
	public InputEventReceiver ier = null;
	
	public FileInput() {
		ier = monitorRuntime;
	}
	
	public boolean tail() {
		return ic.tail;
	}
	
	public boolean isFinite() {
		return ic.tail?false:true;
	}
	
	public FileInput(FileInputConfiguration ic) {
		
		this.log = LogManager.getLogger(FileInput.class);
		this.ic = ic;
		ier = monitorRuntime;
	}

	@Override
	public FileInputConfiguration getInputConfiguration() {
		return this.ic;
	}

	public void run() {
		
		if(ier == null) {
			ier = monitorRuntime;
		}
		
		if (this.ic.repeatInSeconds > 0) {
			
			executorService = Executors.newScheduledThreadPool(1);
			executorService.scheduleAtFixedRate(() -> {
				try {
					if(callback != null) {
						callback.reloadInit();
					}
					ic.reader = new StringReader(String.join(System.lineSeparator(), ic.files));
					//new ReaderInput(id, app, ic);
					reader();

					if(callback != null) {
						callback.reloadFinalize();
					}
				} catch (IOException e) {
					log.error("error during repeated file input init: " + e.getMessage());
					log.debug("error during repeated file input init: " + e.getMessage(), e);
				}
			}, 0, this.ic.repeatInSeconds, TimeUnit.SECONDS);
		} else if(this.ic.tail) {
			try {
				ic.reader = new StringReader(String.join(System.lineSeparator(), ic.files));
				//new ReaderInput(id, app, ic);
				tailReader();
				
			} catch (IOException e) {
				log.error("error during tail-file input init: " + e.getMessage());
				log.debug("error during tail-file input init: " + e.getMessage(), e);
			}
		} else {
			try {

				if(callback != null) {
					callback.reloadInit();
				}
				
				ic.reader = new StringReader(String.join(System.lineSeparator(), ic.files));
				//new ReaderInput(id, app, ic);
				reader();

				if(callback != null) {
					callback.reloadFinalize();
				}
				
				log.debug("regular file input (no tailing, no repetitions) finished");
				monitorRuntime.stopInput(id);
			} catch (IOException e) {
				log.error("error during file input init: " + e.getMessage());
				log.debug("error during file input init: " + e.getMessage(), e);
			}
		}
	}
	
	public void tailReader() throws IOException {

		BufferedReader br = new BufferedReader(ic.reader);
		String line;
		while ((line = br.readLine()) != null) {
			
			File file = new File(line);
			if(isGZipped(file)) {
				// do not try to tail gzipped shit
				log.warn("file '{}' detected as gzip, not tailing this one", line);
			} else {
				TailerListenerAdapterImplementation listener = new TailerListenerAdapterImplementation(this.id);
				Tailer tailer = new Tailer(file, listener, 100, true);
				tailers.add(tailer);
				// stupid executor impl. for demo purposes
				Executor executor = new Executor() {
					public void execute(Runnable command) {
						command.run();
					}
				};

				executor.execute(tailer);
			}
		}
	}
	
	public void reader() throws IOException {
		
		BufferedReader br = new BufferedReader(ic.reader);
		String line = br.readLine();
		
		File file = new File(line.trim());
		
		if(file.exists() && file.isFile()) { // verify that we are dealing with existing files
			
			Map<Integer, Map<String, Object>> filenameMetadataBuffer = new HashMap<Integer, Map<String, Object>>();
			Map<Integer, BufferedReader> readers = new HashMap<Integer, BufferedReader>();
			Map<Integer, Map<String, Object>> contentBuffer = new HashMap<Integer, Map<String, Object>>();
			Map<Integer, Long> sortedIndex = new HashMap<Integer, Long>();

			BufferedReader reader;
			if(isGZipped(file)) {
				InputStream fileStream = new FileInputStream(file);
				InputStream gzipStream = new GZIPInputStream(fileStream);
				Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
				reader = new BufferedReader(decoder);
			} else {
				reader = new BufferedReader(new FileReader(file));
			}
			
			String contentLine = reader.readLine();
			Map<String, Object> parsed = new HashMap<String, Object>();
			if (filenameMetadataBuffer.containsKey(reader.hashCode())) {
				parsed.putAll(filenameMetadataBuffer.get(reader.hashCode()));
			}
			monitorRuntime.parse(contentLine, parsed, true, this.id);
			
			if(parsed.containsKey("logts_timestamp") && parsed.get("logts_timestamp") instanceof Long) {
				sortedIndex.put(reader.hashCode(), (Long)parsed.get("logts_timestamp"));
			} else {
				sortedIndex.put(reader.hashCode(), 0L);
			}
			readers.put(reader.hashCode(), reader);
			contentBuffer.put(reader.hashCode(), parsed);
			
			while ((line = br.readLine()) != null) {
				
				file = new File(line);
				if(isGZipped(file)) {
					InputStream fileStream = new FileInputStream(file);
					InputStream gzipStream = new GZIPInputStream(fileStream);
					Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
					reader = new BufferedReader(decoder);
				} else {
					reader = new BufferedReader(new FileReader(file));
				}
				contentLine = reader.readLine();
				parsed = new HashMap<String, Object>();
				parsed.putAll(filenameMetadataBuffer.get(reader.hashCode()));
				monitorRuntime.parse(contentLine, parsed, true, this.id);
				
				if(parsed.containsKey("logts_timestamp") && parsed.get("logts_timestamp") instanceof Long) {
					sortedIndex.put(reader.hashCode(), (Long)parsed.get("logts_timestamp"));
				} else {
					sortedIndex.put(reader.hashCode(), 0L);
				}
				readers.put(reader.hashCode(), reader);
				contentBuffer.put(reader.hashCode(), parsed);
			}

			/** here should be while loop that reads from different buffers until all buffers are empty **/
			while(sortedIndex.size() > 0) {
				// find out lowest key
				Entry<Integer, Long> lowestEntry = sortedIndex
					.entrySet()
					.stream()
					.min( (x, y) -> Long.compare(x.getValue(), y.getValue()) )
					.get();
				Integer lowestKey = lowestEntry.getKey();

				// we output current first item
				Map<String, Object> cur = contentBuffer.get(lowestKey);
				
				List<String> curMatchedTypes = null;
				curMatchedTypes = (List<String>)cur.get("__match");
				
				cur.put("eventType", (String)curMatchedTypes.get(curMatchedTypes.size()-1));

				if(curMatchedTypes.size() == 0) { // we add default type if somehow nothing exists.. this should not acutally happen
					curMatchedTypes.add("events");
				}
				
				ier.send((String)curMatchedTypes.get(curMatchedTypes.size()-1), cur);
				/* THIS WAS FOR CASE IF USING WITHOUT ESPER, CURRENTLY THIS CAN'T BE USED LIKE SO
				else {
					app.listeners.forEach((k, listener) -> listener.getOutput().printOutput(cur) );
				}*/
				
				// now we re-read a line from current lowestKey buffer to replace previous printout with new data
				contentLine = readers.get(lowestKey).readLine();
				if(contentLine != null) {
					parsed = new HashMap<String, Object>();
					parsed.putAll(filenameMetadataBuffer.get(readers.get(lowestKey).hashCode()));
					monitorRuntime.parse(contentLine, parsed, true, this.id);
					if(parsed.containsKey("logts_timestamp") && parsed.get("logts_timestamp") instanceof Long) {
						sortedIndex.put(readers.get(lowestKey).hashCode(), (Long)parsed.get("logts_timestamp"));
					} else {
						sortedIndex.put(readers.get(lowestKey).hashCode(), 0L);
					}
					contentBuffer.put(readers.get(lowestKey).hashCode(), parsed);
				} else {
					sortedIndex.remove(lowestKey);
					filenameMetadataBuffer.remove(lowestKey);
					readers.remove(lowestKey);
					contentBuffer.remove(lowestKey);
				}
			}
		} else {
			log.warn("input '{}' is not an existing file for file-input ... ", file.getAbsolutePath());
		}
	}

	public class TailerListenerAdapterImplementation extends TailerListenerAdapter {
		String id;
		TailerListenerAdapterImplementation(String id) { 
			super();
			this.id = id;
			
		}
		
		public void handle(String line) {
			Map<String, Object> parsed = new HashMap<String, Object>();
			monitorRuntime.parse(line, parsed, true, this.id);
			ier.send((String)parsed.get("eventType"), parsed);
			//monitorRuntime.parseAndSend(line, null, id);
		}
	}
	
	/**
	 * Checks if a file is gzipped.
	 * 
	 * @param f
	 * @return
	 */
	private static boolean isGZipped(File f) {
		int magic = 0;
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			magic = raf.read() & 0xff | ((raf.read() << 8) & 0xff00);
			raf.close();
		} catch (Throwable e) {
			// totally normal
		}
		return magic == GZIPInputStream.GZIP_MAGIC;
	}

	@Override
	public void shutdown() {
		if(this.executorService != null && !this.executorService.isShutdown()) {
			log.debug("stopping repeated file input scheduler");
			this.executorService.shutdown();
		}
		if(this.tailers.size() > 0) {
			log.debug("stopping file tailers");
			for(Tailer tailer : this.tailers) {
				tailer.stop();
			}
		}
		log.debug("stopped file input");
	}
	
	public String getDescription() {
		return "File (" + String.join(",", ic.files) + ")";
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
