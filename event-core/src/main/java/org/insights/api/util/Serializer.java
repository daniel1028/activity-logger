package org.insights.api.util;

import org.insights.api.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import flexjson.JSONDeserializer;

public class Serializer {

	private static Gson gson = new Gson();
	
	private static final Logger logger = LoggerFactory
			.getLogger(Serializer.class);

	public static Event buildEventObject(String data) {
		try {
			return data != null ? gson.fromJson(data, Event.class) : null;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception : ", e);
		}
		return null;
	}

	public static <T> T deserialize(String json, Class<T> clazz) {
		try {
			return new JSONDeserializer<T>().use(null, clazz).deserialize(json);
		} catch (Exception e) {
			e.printStackTrace();

			System.out.print("Exception : " + e);
			logger.error("Exception : ", e);
		}
		return null;
	}
}
