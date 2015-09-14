package org.log.kafka.consumer;

import java.util.Properties;

import org.insights.api.model.Event;
import org.insights.api.service.DataLoaderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class CassandraProcessor extends BaseDataProcessor implements
		DataProcessor {
	protected Properties properties;
	@Autowired
	protected DataLoaderService dataLoader;
	static final Logger logger = LoggerFactory
			.getLogger(CassandraProcessor.class);

	@Override
	public void handleRow(Object row) throws Exception {

		if (row != null && (row instanceof Event)) {
			Event event = (Event) row;

			if (event.getEventName() == null || event.getEventName().isEmpty()
					|| event.getContext() == null) {
				logger.warn("EventName or Context is empty. This is an error in EventObject");
				return;
			}
			dataLoader.processMessage(event);
		}
	}
}
