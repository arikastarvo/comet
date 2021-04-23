package com.github.arikastarvo.comet.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.arikastarvo.comet.MonitorRuntime;

public class PersistenceManager {

	Logger log = LoggerFactory.getLogger(PersistenceManager.class);
	
	ScheduledExecutorService executorService;
	MonitorRuntime monitorRuntime;
	
	private static List<String> allowedTypes = Arrays.asList("file");
	
	private Persistence persistenceStorage;
	
	private PersistenceManager() {}

	public PersistenceManager(MonitorRuntime monitorRuntime) throws Exception {
		this.monitorRuntime = monitorRuntime;
		
		if(!allowedTypes.contains(monitorRuntime.configuration.persistenceConfiguration.storageType)) {
			throw new Exception("unsupported persistence storage type");
		}
		
		switch(monitorRuntime.configuration.persistenceConfiguration.storageType) {
			
			default:
				persistenceStorage = new FilePersistence(monitorRuntime.configuration.getDefaultDataPath());
				break;
				
		}
		
		this.log = LoggerFactory.getLogger(PersistenceManager.class);
		this.executorService = Executors.newScheduledThreadPool(1);
	}

	public void loadAndSchedulePeristence() {
		log.debug("starting persistence manager");
		if(monitorRuntime.configuration.persistenceConfiguration.initialLoad) {
			persistenceLoad();
		}

		if(monitorRuntime.configuration.persistenceConfiguration.persistenceInterval > 0) {
			scheduledPersist();
		}
	}
	
	public void persistenceLoad() {
	
		
		for(String eventTypeName : monitorRuntime.configuration.persistenceConfiguration.persistence) {
			log.debug("start loading persistence data for '{}'", eventTypeName);
			List<Map<String, Object>> objects = persistenceStorage.load(eventTypeName);
			try {
				if(objects != null && objects.size() > 0) {
					for(Map<String, Object> object : objects) {
	
						// how to check if event type exists?
						//List<String> values = new ArrayList<String>();
						List<String> values = object.values().stream().map( (Object value ) -> {
							if(value == null) {
								return "''";
							} else if(value instanceof String) {
								return String.format("'%s'", (String)value);
							} else if(value instanceof Long) {
								return ((Long)value).toString();
							} else {
								return String.format("'%s'", value.toString());
							}
						}).collect(Collectors.toList());
						
						String vals = String.join(",", new ArrayList(values));
						String keys = String.join(",", new ArrayList(object.keySet()));
	
						String query = String.format("@Tag(name='silent', value='true') insert into %s (%s) values (%s)", eventTypeName, keys, vals);
						try {
							monitorRuntime.fireAndForget(query, false);
							//monitorRuntime.intoRuntime(eventTypeName, object);
						} catch (Exception e) {
							log.error("inserting persistence data to esper failed for '{}' : {}", eventTypeName, e.getMessage());
							log.debug("inserting persistence data to esper failed for '{}' : {}", eventTypeName, e.getMessage(), e);
						}
					}
				}
			} catch (Exception e) {
				log.error("loading persistence data failed for '{}' : {}", eventTypeName, e.getMessage());
				log.debug("loading persistence data failed for '{}' : {}", eventTypeName, e.getMessage(), e);
			}
			if(objects != null && objects.size() > 0) {
				log.debug("loaded {} items for '{}' (if no errors occured)", objects.size(), eventTypeName);
			}
		}
	}

	
	public void scheduledPersist() {
		if(monitorRuntime.configuration.persistenceConfiguration.persistence != null && monitorRuntime.configuration.persistenceConfiguration.persistence.size() > 0) {
			executorService.scheduleAtFixedRate(() -> {
				this.persistencePersist();
			}, monitorRuntime.configuration.persistenceConfiguration.persistenceInterval, monitorRuntime.configuration.persistenceConfiguration.persistenceInterval, TimeUnit.SECONDS);
		}
	}
	
	public void stopScheduledPersist() {
		executorService.shutdown();
	}
	
	public void persistencePersist() {
		
		for(String eventTypeName : monitorRuntime.configuration.persistenceConfiguration.persistence) { 

			log.debug("start persisting data for '{}'", eventTypeName);
			// how to check if event type exists?
			String query = String.format("select * from %s", eventTypeName);
			try {
				List<Map<String, Object>> objects = monitorRuntime.fireAndForget(query, false);
				
				try {
					persistenceStorage.persist(objects, eventTypeName);
					log.debug("persisted {} items for '{}'", objects.size(), eventTypeName);
				} catch (Exception e) {
					log.debug("persisting failed for '{}'", eventTypeName);
				}
			} catch (Exception e) {
				log.error("Could not persist {}, event probably doesn't exists? ERR - {}", eventTypeName, e.getMessage(), e);
				log.debug("Could not persist {}, event probably doesn't exists? ERR - {}", eventTypeName, e.getMessage(), e);
			}

		}
	}

}
