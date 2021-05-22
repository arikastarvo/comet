package com.github.arikastarvo.comet.output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.github.arikastarvo.comet.MonitorRuntimeConfiguration;
import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

public abstract class OutputConfiguration<T extends OutputConfiguration<T>> extends HashMap<String, Object> {


	private final static String OUTPUT_BASE_PACKAGE = "com.github.arikastarvo.comet.output"; 
	private final static String OUTPUT_ANNOTATION_CLASS = "com.github.arikastarvo.comet.output.OutputConnector"; 
	
	protected static Logger log;
	
	protected MonitorRuntimeConfiguration monitorRuntimeConfiguration;
	
	public String name;
	
	protected OutputConfiguration() { }
	
	public OutputConfiguration(MonitorRuntimeConfiguration monitorRuntimeConfiguration) {
		super();
		this.monitorRuntimeConfiguration = monitorRuntimeConfiguration;
	}

	public static <T extends OutputConfiguration<T>> List<Class<T>> getOutputConfigurationClasses() {
		List<Class<T>> classes = new ArrayList<Class<T>>();
		
		try {
			ScanResult scanResult = new ClassGraph()
	        	.verbose()
	            .enableAllInfo()
	            .acceptPackages(OutputConfiguration.OUTPUT_BASE_PACKAGE)
	            .scan();

		    for (ClassInfo routeClassInfo : scanResult.getClassesWithAnnotation(OutputConfiguration.OUTPUT_ANNOTATION_CLASS)) {
		    	AnnotationInfo annotationInfo = routeClassInfo.getAnnotationInfo(OutputConfiguration.OUTPUT_ANNOTATION_CLASS);
		    	OutputConnector outputConnector = (OutputConnector) annotationInfo.loadClassAndInstantiate();
		    	//AnnotationParameterValue paramValue = annotationInfo.getParameterValues().get("configuration");
		        classes.add((Class<T>)outputConnector.configuration());
		    }
		} catch (Exception e) {
			log.error("loading output plugins failed: {}", e.getMessage());
			log.debug("loading output plugins failed: {}", e.getMessage(), e);
		}
		return classes;
	}

	public abstract String getOutputType();
	public abstract OutputConfiguration<T> parseMapOutputDefinition(Map<String, Object> outputDefinition) throws OutputDefinitionException;
	public abstract Output createOutputInstance();
	
}

