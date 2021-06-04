package com.github.arikastarvo.comet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.arikastarvo.comet.InputEventReceiver;

public class InMemoryInputEventReceiver implements InputEventReceiver {

    public List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();

    @Override
    public void send(String type, Map<String, Object> data) {
        this.data.add(new HashMap<String, Object>() {{
            put("type", type);
            put("data", data);
        }});
    }
    
}
