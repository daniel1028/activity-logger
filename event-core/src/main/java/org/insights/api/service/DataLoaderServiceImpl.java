package org.insights.api.service;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.insights.api.constants.ColumnFamily;
import org.insights.api.daos.CassandraDAO;
import org.insights.api.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
		String eventKeyUUID = getCassandraDAO().saveEvent(ColumnFamily.EVENTDETAIL.getColumnFamily(), null, event);
		if (eventKeyUUID == null) {
			return;
		}
		Date eventDateTime = new Date(event.getEndTime());
		String eventRowKey = minuteDateFormatter.format(eventDateTime).toString();
		getCassandraDAO().updateTimelineObject(ColumnFamily.EVENTTIMELINE.getColumnFamily(), eventRowKey, eventKeyUUID.toString(), event);
		
		getCassandraDAO().indexEvent(event);
	}
}
