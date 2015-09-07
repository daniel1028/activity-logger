package org.insights.api.model;

import java.io.Serializable;

public class Event implements Serializable {

	/**
	 * @author daniel
	 */
	private static final long serialVersionUID = -1438385342930630628L;

	private String context;
	
	private String user;
	
	private String payLoadObject;
	
	private String metrics;
	
	private String session;

	private Long startTime;
	
	private Long endTime;
	
	// Added Specially for Creating the Event
	private String apiKey;
	
	private String eventName;
	
	private String contentGooruId;
	
	private String parentGooruId;
	
	private Long timeInMillSec;
	
	private String fields;
	
	private String eventId;
	
	private String parentEventId;
	
	private String version;

	private String eventType;

	private String organizationUid;
	
	private Long hitCount;
	
	public Long getHitCount() {
		return hitCount;
	}

	public void setHitCount(Long hitCount) {
		this.hitCount = hitCount;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getOrganizationUid() {
		return organizationUid;
	}

	public void setOrganizationUid(String organizationUid) {
		this.organizationUid = organizationUid;
	}

	public String getParentEventId() {
		return parentEventId;
	}

	public void setParentEventId(String parentEventId) {
		this.parentEventId = parentEventId;
	}

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

	public String getContentGooruId() {
		return contentGooruId;
	}

	public void setContentGooruId(String contentGooruId) {
		this.contentGooruId = contentGooruId;
	}

	public String getParentGooruId() {
		return parentGooruId;
	}

	public void setParentGooruId(String parentGooruId) {
		this.parentGooruId = parentGooruId;
	}

	public Long getTimeInMillSec() {
		return timeInMillSec;
	}

	public void setTimeInMillSec(Long timeInMillSec) {
		this.timeInMillSec = timeInMillSec;
	}

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getContext() {
		return context;
	}

	public void setContext(String context) {
		this.context = context;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPayLoadObject() {
		return payLoadObject;
	}

	public void setPayLoadObject(String payLoadObject) {
		this.payLoadObject = payLoadObject;
	}

	public String getMetrics() {
		return metrics;
	}

	public void setMetrics(String metrics) {
		this.metrics = metrics;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getApiKey() {
		return apiKey;
	}

	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}

}
