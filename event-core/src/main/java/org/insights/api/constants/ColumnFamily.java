package org.insights.api.constants;

	public enum ColumnFamily {

		APIKEY("app_api_key"),
		
		EVENTDETAIL("event_detail"),
		
		TAXONOMYCODE("taxonomy_code"),
		
		TABLEDATATYPES("table_datatypes"),
		
		EVENTTIMELINE("event_timeline"),
		
		ACTIVITYSTREAM("activity_stream"),
		
		DIMEVENTS("dim_events_list"),
		
		DIMDATE("dim_date"),
		
		DIMTIME("dim_time"),
		
		DIMUSER("dim_user"),
		
		EXTRACTEDUSER("extracted_user"),
		
		EXTRACTEDCODE("extracted_code"),
		
		DIMCONTENTCLASSIFICATION("dim_content_classification"),
		
		DIMRESOURCE("dim_resource"),
		
		STAGING("staging_event_detail"),
		
		EVENTFIELDS("event_fields"),
		
		CONFIGSETTINGS("job_config_settings"),
		
		JOB_TRACKER("job_tracker"),
		
		REALTIMECONFIG("real_time_operation_config"),
		
		RECENTVIEWEDRESOURCES("recent_viewed_resources"),
		
		LIVEDASHBOARD("live_dashboard"),
		
		MICROAGGREGATION("micro_aggregation"),
		
		ACITIVITYSTREAM("activity_stream"),
		
		REALTIMECOUNTER("real_time_counter"),
		
		REALTIMEAGGREGATOR("real_time_aggregator"),
		
		QUESTIONCOUNT("question_count"),
		
		COLLECTIONITEM("collection_item"),
		
		COLLECTIONITEMASSOC("collection_item_assoc"),
		
		COLLECTION("collection"),
		
		CLASSPAGE("classpage"),
		
		LICENSE("license"),
		
		RESOURCETYPES("resource_type"),
		
		RESOURCEFORMAT("resource_format"),

		INSTRUCTIONAL("instructional_use"),
		
		CATEGORY("category"),
		
		RESOURCE("resource"),
		
		ASSESSMENTANSWER("assessment_answer"),
		
		USER("user"),
		
		ANONYMIZEDUSERDATA("anonymized_user_data"),
		
		SESSIONS("sessions"),
		
		SESSION_ACTIVITY("session_activity"),
		
		SESSION_ACTIVITY_COUNTER("session_activity_counter"),
		
		CLASS_ACTIVITY("class_activity"),
		
		CLASS_ACTIVITY_COUNTER("class_activity_counter"),
		
		CONTENT_META("content_meta"),
		
		USER_GROUP_ASSOCIATION("user_group_association"),
		;
		
		String name;

		
		private ColumnFamily(String name) {
			this.name = name;
		}


		public String getColumnFamily(){
			return name;
		}

	}

