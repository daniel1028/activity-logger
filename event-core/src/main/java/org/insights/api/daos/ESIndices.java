package org.insights.api.daos;

public enum ESIndices {
	
	EVENTLOGGERINFO("event_logger_info" , "event_detail"),
	
	;
	
	String name;
	
	String type;
	
	private ESIndices(String name, String type) {
		this.name = name;
		this.type = type;
	}
	
	private ESIndices(String name) {
		this.name = name;
		this.type = name;
	}

	public String getIndex() {
		return name;
	}

	public String getType() {
		return type;
	}
}
