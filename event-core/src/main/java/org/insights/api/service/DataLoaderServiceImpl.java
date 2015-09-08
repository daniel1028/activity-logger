package org.insights.api.service;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.insights.api.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.insights.api.util.JSONDeserializer;
import org.insights.api.constants.*;
import org.insights.api.daos.CassandraDAO;

import com.netflix.astyanax.util.TimeUUIDUtils;

@Service
public class DataLoaderServiceImpl implements DataLoaderService {

	private static final Logger logger = LoggerFactory.getLogger(DataLoaderServiceImpl.class);
	
	@Autowired
	private CassandraDAO cassandraDao;

	public CassandraDAO getCassandraDAO() {
		return cassandraDao;
	}
	
	private static final SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
	
	@Override
	public void processMessage(Event event) {
		Map<String, Object> eventMap = new LinkedHashMap<String, Object>();
		eventMap = JSONDeserializer.deserializeEvent(event);
		String eventName = (String) eventMap.get(Constants.EVENT_NAME);
		String apiKey = event.getApiKey() != null ? event.getApiKey() : Constants.DEFAULT_API_KEY;
		Map<String, Object> records = new HashMap<String, Object>();
		records.put(Constants._EVENT_NAME, eventName);
		records.put(Constants._API_KEY, apiKey);
		Collection<String> eventId = getCassandraDAO().getKey(ColumnFamily.DIMEVENTS.getColumnFamily(), records);
		if (eventId == null || eventId.isEmpty()) {
			UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			records.put(Constants._EVENT_ID, uuid.toString());
			String key = apiKey + Constants.SEPERATOR + uuid.toString();
			getCassandraDAO().saveBulkList(ColumnFamily.DIMEVENTS.getColumnFamily(), key, records);
		}

		String eventKeyUUID = getCassandraDAO().saveEvent(ColumnFamily.EVENTDETAIL.getColumnFamily(), null, event);
		if (eventKeyUUID == null) {
			return;
		}

		Date eventDateTime = new Date(event.getEndTime());
		String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
		getCassandraDAO().updateTimelineObject(ColumnFamily.EVENTTIMELINE.getColumnFamily(), eventRowKey, eventKeyUUID.toString(), event);

	}

}
