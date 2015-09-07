/*******************************************************************************
 * JSONProcessor.java
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

import org.apache.commons.lang.StringUtils;
import org.insights.api.model.Event;

import com.google.gson.Gson;

public class JSONProcessor extends BaseDataProcessor implements DataProcessor {
    private Gson gson;

    public JSONProcessor() {
    	this.gson = new Gson();
	}
    
	@Override
	public void handleRow(Object row) throws Exception {
		String jsonRowObject = (String)row;
		
		if(StringUtils.isEmpty(jsonRowObject)) {
			return;
		}
		
		long startTime = System.currentTimeMillis(), firstStartTime = System.currentTimeMillis();
		
		if(row == null) {
			LOG.error("The row was null. This is invalid");
			return;
		}
		
		// Override and set fields to be the original log message / JSON. 
	        Event event = null;
	        try {
	            event = gson.fromJson(jsonRowObject, Event.class);
	            event.setFields(jsonRowObject);        
	            getNextRowHandler().processRow(event);
	        } catch (Exception e) {
	            LOG.error("Had a problem trying to parse EventObject JSON from the raw line {}", jsonRowObject, e);
	            return;
	        }

	        
		long partEndTime = System.currentTimeMillis();
		LOG.trace("Cassandra update: {} ms : Total : {} ms " , (partEndTime - startTime),  (partEndTime  - firstStartTime));
	}	
}
