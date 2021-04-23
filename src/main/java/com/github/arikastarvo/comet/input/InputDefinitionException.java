package com.github.arikastarvo.comet.input;

public class InputDefinitionException extends Exception {
	
	private static final long serialVersionUID = -4321312380310184103L;

	public InputDefinitionException(String message) {
		super(message);
	}
	
	public InputDefinitionException(String message, Exception e) {
		super(message, e);
	}
}
