package org.insights.api.service;

import org.insights.api.model.Event;

public interface DataLoaderService {

	public void processMessage(Event event);
}
