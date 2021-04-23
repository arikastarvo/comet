package com.github.arikastarvo.comet;

import java.util.Map;

public interface InputEventReceiver {

	public void send(String type, Map<String, Object> data);
	
}
