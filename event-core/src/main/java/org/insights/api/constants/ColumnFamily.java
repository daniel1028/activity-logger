package org.insights.api.constants;

	public enum ColumnFamily {

		EVENTDETAIL("event_detail"),
		
		EVENTTIMELINE("event_timeline"),

		DIMEVENTS("dim_events_list"),

		;
		
		String name;

		
		private ColumnFamily(String name) {
			this.name = name;
		}


		public String getColumnFamily(){
			return name;
		}

	}

