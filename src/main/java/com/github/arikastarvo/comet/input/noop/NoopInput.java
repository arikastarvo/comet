package com.github.arikastarvo.comet.input.noop;

import org.apache.logging.log4j.LogManager;

import com.github.arikastarvo.comet.input.FiniteInput;
import com.github.arikastarvo.comet.input.Input;
import com.github.arikastarvo.comet.input.InputConnector;
/**
 * Does nothing and keeps running
 * 
 * Can be used if some other input type dies and is terminated for some reason but application should not exit.
 * 
 * @author tarvo
 *
 */
@InputConnector(
	name = NoopInput.NAME,
	configuration = NoopInputConfiguration.class
)
public class NoopInput extends Input<NoopInput> implements FiniteInput {

	public static final String NAME = "noop";

	NoopInputConfiguration ic;
	
	/*public NoopInput() {
		this.log = LogManager.getLogger(NoopInput.class);
	}*/
	
	public NoopInput(NoopInputConfiguration ic) {
		this.log = LogManager.getLogger(NoopInput.class);
		this.ic = ic;
	}
	
	@Override
	public NoopInputConfiguration getInputConfiguration() {
		return this.ic;
	}

	public void run() {
		this.log = LogManager.getLogger(NoopInput.class);
		log.debug("starting DummyInput as " + (ic.finite?"finite":"infinite"));
		
		if(!ic.finite) {
		    try {
				Thread.currentThread().join();
			} catch (InterruptedException e) {
				// nothing to report here, normal operation during shutdown process
			}
		}
		Stop();
	}

	@Override
	public void shutdown() {
		log.debug("stopping dummy input");
		Thread.currentThread().interrupt();
	}
	
	public String getDescription() {
		return "Dummy (noop)";
	}

	@Override
	public boolean isFinite() {
		return ic.finite;
	}
}
