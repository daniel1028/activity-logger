package org.insights.api.service;

import org.insights.api.model.Event;

public interface EventService {

	public void processMessage(Event event);
}
