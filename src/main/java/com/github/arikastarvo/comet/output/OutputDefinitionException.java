package com.github.arikastarvo.comet.output;

public class OutputDefinitionException extends Exception {
	
	private static final long serialVersionUID = 2665991676742311919L;

	public OutputDefinitionException(String message) {
		super(message);
	}
	
	public OutputDefinitionException(String message, Exception e) {
		super(message, e);
	}
}
