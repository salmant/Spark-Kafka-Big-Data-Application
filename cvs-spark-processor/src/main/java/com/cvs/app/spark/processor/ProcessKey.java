package com.cvs.app.spark.processor;

import java.io.Serializable;

// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------

/*
 * Class to have two attributes (travelId and eventType).
 * This is because in order to process the stream, the number of each driving dynamics type is identified by travelId and eventType.
 * ProcessKey object is a key in countDStreamPair defined as JavaPairDStream<ProcessKey, Long>. 
 */
public class ProcessKey implements Serializable {
	
	private String travelId;
	private String eventType;
	
	public ProcessKey(String travelId, String eventType) {
		super();
		this.travelId = travelId;
		this.eventType = eventType;
	}

	public String gettravelId() {
		return travelId;
	}

	public String geteventType() {
		return eventType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((travelId == null) ? 0 : travelId.hashCode());
		result = prime * result + ((eventType == null) ? 0 : eventType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj !=null && obj instanceof ProcessKey){
			ProcessKey other = (ProcessKey)obj;
			if(other.gettravelId() != null && other.geteventType() != null){
				if((other.gettravelId().equals(this.travelId)) && (other.geteventType().equals(this.eventType))){
					return true;
				} 
			}
		}
		return false;
	}
	
}
