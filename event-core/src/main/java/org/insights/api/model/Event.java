package org.insights.api.model;

import java.io.Serializable;
import java.util.Map;

public class Event implements Serializable {

	/**
	 * @author daniel
	 */
	private static final long serialVersionUID = -1438385342930630628L;

	private Map<String, Object> context;
	
	private Map<String, Object> user;
	
	private Map<String, Object> payLoadObject;
	
	private Map<String, Object> metrics;
	
	private Map<String, Object> session;

	private Long startTime;
	
	private Long endTime;
	
	private String eventName;
	
	private String eventId;

	private String fields;

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public Map<String, Object> getContext() {
		return context;
	}

	public void setContext(Map<String, Object> context) {
		this.context = context;
	}

	public Map<String, Object> getUser() {
		return user;
	}

	public void setUser(Map<String, Object> user) {
		this.user = user;
	}

	public Map<String, Object> getPayLoadObject() {
		return payLoadObject;
	}

	public void setPayLoadObject(Map<String, Object> payLoadObject) {
		this.payLoadObject = payLoadObject;
	}

	public Map<String, Object> getMetrics() {
		return metrics;
	}

	public void setMetrics(Map<String, Object> metrics) {
		this.metrics = metrics;
	}

	public Map<String, Object> getSession() {
		return session;
	}

	public void setSession(Map<String, Object> session) {
		this.session = session;
	}

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}
}
