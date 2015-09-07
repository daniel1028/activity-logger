package org.insights.api.service;

import org.insights.api.constants.Constants;
import org.insights.api.model.Event;
import org.insights.api.util.ServerValidationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EventServiceImpl implements EventService,Constants {

	@Autowired
	private DataLoaderService dataLoaderService;
	
	@Override
	public void processMessage(Event event) {
		Boolean isValidEvent = validateInsertEvent(event);
		if (isValidEvent) {
			dataLoaderService.processMessage(event);
		}
		
	}
	private Boolean validateInsertEvent(Event event) {
		Boolean isValidEvent = true;
		if (event == null) {
			ServerValidationUtils.logErrorIfNull(isValidEvent, event, "event.all", RAW_EVENT_NULL_EXCEPTION);
		}
				
		return isValidEvent;
	}

}
