package org.insights.api.daos;

import java.util.Collection;
import java.util.Map;

import org.insights.api.model.Event;


public interface CassandraDAO {
	
	public Collection<String> getKey(String cfName, Map<String, Object> columns);
	
	public void saveBulkList(String cfName, String key, Map<String, Object> columnValueList);
	
	public String saveEvent(String cfName, String key, Event event);
	
	public void updateTimelineObject(String cfName, String rowKey, String CoulmnValue, Event event);
	
}
