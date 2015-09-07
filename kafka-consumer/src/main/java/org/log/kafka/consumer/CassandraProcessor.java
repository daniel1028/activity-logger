/*******************************************************************************
 * CassandraProcessor.java
 * insights-event-logger
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.log.kafka.consumer;

import java.util.Properties;

import org.insights.api.model.Event;
import org.insights.api.service.DataLoaderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.Gson;

public class CassandraProcessor extends BaseDataProcessor implements
		DataProcessor {
	protected Properties properties;
	@Autowired
	protected DataLoaderService dataLoader;
	private Gson gson = new Gson();
	private final String GOORU_EVENT_LOGGER_API_KEY = "5673eaa7-15e3-4d6b-b3ef-5f7729c82de3";
	private final String EVENT_SOURCE = "kafka-logged";
	static final Logger logger = LoggerFactory.getLogger(CassandraProcessor.class);

	@Override
	public void handleRow(Object row) throws Exception {

        if (row != null && (row instanceof Event)) {
       	
       	 Event event = (Event) row;
        	
       	 if(event.getVersion() == null){
            	return;
            }
       	 
       	if (event.getEventName() == null || event.getEventName().isEmpty() || event.getContext() == null) {
       		logger.warn("EventName or Context is empty. This is an error in EventObject");
       		return;
        	}

//       	eventObjectValidator.validateEventObject(eventObject);
        	dataLoader.processMessage(event);
        }
	}
}
