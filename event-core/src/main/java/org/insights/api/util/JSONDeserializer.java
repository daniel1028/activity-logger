package org.insights.api.util;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.insights.api.constants.Constants;
import org.insights.api.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JSONDeserializer implements Constants {

	private static final Logger logger = LoggerFactory.getLogger(JSONDeserializer.class);

	private static Gson gson = new Gson();

	public static <T> T deserializeEvent(Event event) {
        Type mapType = new TypeToken <HashMap<String, Object>>() {}.getType();
        Type numberMapType = new TypeToken <HashMap<String, Number>>() {}.getType();
        Map<String,Object> map = new HashMap<String,Object>();
        try {
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getUser(), mapType));
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getMetrics(), numberMapType));
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getPayLoadObject(), mapType));
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getContext(), mapType));
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getSession(), mapType));
                map.put(EVENT_NAME,event.getEventName());
                map.put(EVENT_ID,event.getEventId());
                map.put(START_TIME,event.getStartTime());
                map.put(END_TIME,event.getEndTime());
        } catch (Exception e) {
        		logger.info("Exception in event : {}",event.getFields());
                logger.error("Exception:", e);
        }
        return (T) map;
	}

}
