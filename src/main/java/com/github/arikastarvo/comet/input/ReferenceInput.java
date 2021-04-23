package com.github.arikastarvo.comet.input;

import com.github.arikastarvo.comet.InputEventReceiver;
import com.github.arikastarvo.comet.reference.ReferenceReloadCallback;

public interface ReferenceInput {
	
	public void setInputEventReceiver(InputEventReceiver ier);
	
	public void setReloadCallback(ReferenceReloadCallback callback);
}
