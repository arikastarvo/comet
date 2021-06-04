package com.github.arikastarvo.comet.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import com.github.arikastarvo.comet.input.csv.CSVInputConfiguration;
import com.github.arikastarvo.comet.input.file.FileInputConfiguration;
import com.github.arikastarvo.comet.input.list.StaticListInputConfiguration;
import com.github.arikastarvo.comet.input.noop.NoopInputConfiguration;
import com.github.arikastarvo.comet.input.stdin.StdinInputConfiguration;

import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

public abstract class InputConfiguration<T extends InputConfiguration<T>> extends HashMap<String, Object> {

	private final static String INPUT_BASE_PACKAGE = "com.github.arikastarvo.comet.input"; 
	private final static String INPUT_ANNOTATION_CLASS = "com.github.arikastarvo.comet.input.InputConnector"; 
	
	protected static Logger log;
	
	protected MonitorRuntimeConfiguration monitorRuntimeConfiguration;
	
	public String name;
	
	protected InputConfiguration() { }
	private InputConfiguration(Object[] args) { }
	
	public InputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) {
		super();
		this.monitorRuntimeConfiguration = monitorRuntimeConfiguration;
	}

	public MonitorRuntimeConfiguration getMonitorRuntimeConfiguration() {
		return this.monitorRuntimeConfiguration;
	}
	
	public static <T extends InputConfiguration<T>> List<Class<T>> getInputConfigurationClasses() {

		List<Class<T>> classes = new ArrayList<Class<T>>();
		
		try {
			ScanResult scanResult = new ClassGraph()
	        	.verbose()
	            .enableAllInfo()
	            .acceptPackages(InputConfiguration.INPUT_BASE_PACKAGE)
	            .scan();

		    for (ClassInfo routeClassInfo : scanResult.getClassesWithAnnotation(InputConfiguration.INPUT_ANNOTATION_CLASS)) {
		    	AnnotationInfo annotationInfo = routeClassInfo.getAnnotationInfo(InputConfiguration.INPUT_ANNOTATION_CLASS);
		    	InputConnector inputConnector = (InputConnector) annotationInfo.loadClassAndInstantiate();
		    	//AnnotationParameterValue paramValue = annotationInfo.getParameterValues().get("configuration");
		        classes.add((Class<T>)inputConnector.configuration());
		    }
		} catch (Exception e) {
			log.error("loading input plugins failed: {}", e.getMessage());
			log.debug("loading input plugins failed: {}", e.getMessage(), e);
		}
		return classes;
	}

	public abstract String getInputType();
	public abstract InputConfiguration<T> parseMapInputDefinition(Map<String, Object> inputDefinition) throws InputDefinitionException;
	//public abstract <S extends Input<S>> Class<S> getInputClass();
	public abstract Input createInputInstance();
	
}

