package com.cvs.app.spark.processor;

import java.util.Date;
import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonFormat;

// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------

public class WindowBasedInfo implements Serializable {
	
	private String travelId;
	private String eventType;
	private long totalCount;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
	private Date timeStamp;
	private String recordDate;
	
	public String gettravelId() {
		return travelId;
	}
	
	public void settravelId(String travelId) {
		this.travelId = travelId;
	}
	
	public String geteventType() {
		return eventType;
	}
	
	public void seteventType(String eventType) {
		this.eventType = eventType;
	}
	
	public long getTotalCount() {
		return totalCount;
	}
	
	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}
	
	public Date getTimeStamp() {
		return timeStamp;
	}
	
	public void setTimeStamp(Date timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	public String getRecordDate() {
		return recordDate;
	}

	public void setRecordDate(String recordDate) {
		this.recordDate = recordDate;
	}

}
