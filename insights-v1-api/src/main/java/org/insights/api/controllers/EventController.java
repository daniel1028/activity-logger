package org.insights.api.controllers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.insights.api.constants.Constants;
import org.insights.api.model.Event;
import org.insights.api.service.EventService;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

@Controller
@RequestMapping(value = "/event")
@EnableAsync
public class EventController implements AsyncConfigurer, Constants {

	protected final Logger logger = LoggerFactory
			.getLogger(EventController.class);

	@Autowired
	protected EventService eventService;

	private Gson gson = new Gson();

	@RequestMapping(method = RequestMethod.POST, headers = "Content-Type=application/json")
	public void trackEvent(@RequestBody String fields,
			@RequestParam(value = "apiKey", required = true) String apiKey,
			HttpServletRequest request, HttpServletResponse response)
			throws Exception {

		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers",
				"Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");
		JsonElement jsonElement = null;
		JsonArray eventJsonArr = null;

		if (!fields.isEmpty()) {
			try {
				jsonElement = new JsonParser().parse(fields);
				eventJsonArr = jsonElement.getAsJsonArray();
			} catch (JsonParseException e) {
				sendErrorResponse(request, response,
						HttpServletResponse.SC_BAD_REQUEST, INVALID_JSON);
				logger.error(INVALID_JSON, e);
				return;
			}
		} else {
			sendErrorResponse(request, response,
					HttpServletResponse.SC_BAD_REQUEST, BAD_REQUEST);
			return;
		}
		request.setCharacterEncoding("UTF-8");
		Long timeStamp = System.currentTimeMillis();
		String userAgent = request.getHeader("User-Agent");
		String userIp = request.getHeader("X-FORWARDED-FOR");
		if (userIp == null) {
			userIp = request.getRemoteAddr();
		}
		for (JsonElement eventJson : eventJsonArr) {
			JsonObject eventObj = eventJson.getAsJsonObject();
			String eventString = eventObj.toString();
			Event event = gson.fromJson(eventObj, Event.class);
			JSONObject field = new JSONObject(eventString);
			if (StringUtils.isNotBlank(event.getUser())) {
				JSONObject user = new JSONObject(event.getUser());
				user.put(USER_IP, userIp);
				user.put(USER_AGENT, userAgent);
				field.put(USER, user.toString());
			}
			event.setFields(field.toString());
			event.setApiKey(apiKey);
			eventService.processMessage(event);
		}

		return;
	}

	private void sendErrorResponse(HttpServletRequest request,
			HttpServletResponse response, int responseStatus, String message) {
		response.setStatus(responseStatus);
		response.setContentType("application/json");
		Map<String, Object> resultMap = new HashMap<String, Object>();

		resultMap.put("statusCode", responseStatus);
		resultMap.put("message", message);
		JSONObject resultJson = new JSONObject(resultMap);

		try {
			response.getWriter().write(resultJson.toString());
		} catch (IOException e) {
			logger.error("OOPS! Something went wrong", e);
		}
	}

	@Override
	public Executor getAsyncExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(10);
		executor.setMaxPoolSize(50);
		executor.setQueueCapacity(100);
		executor.setThreadNamePrefix("eventExecutor");
		executor.initialize();
		return executor;
	}

}
