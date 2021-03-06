package com.cvs.app.spark.processor;

import java.io.Serializable;
import java.util.Date;
import com.fasterxml.jackson.annotation.JsonFormat;

// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------

/*
 * Class to represent the CVS data message.
 */

//Kafka stores and transports byte arrays in its topics.
//Serialization is a mechanism of converting the state of an object into a byte stream. Only the objects of those classes can be serialized which are implementing java.io.Serializable interface.
//The byte stream created is platform independent. So, the object serialized on one platform can be deserialized on a different platform.
public class CVSData implements Serializable {
	
	private String driverId;
	private String travelId;
	private String dateX;
	private String timeX;
	private String lat;
	private String lon;
	private double speed;
	private String eventType;
	
	public CVSData(){
		
	}
	
	public CVSData(String driverId, String travelId, String dateX, String timeX, String lat, String lon, double speed, String eventType) {
		super();
		this.driverId = driverId;
		this.travelId = travelId;
		this.dateX = dateX;
		this.timeX = timeX;
		this.lat = lat;
		this.lon = lon;
		this.speed = speed;
		this.eventType = eventType;
	}
	
	public String getdriverId() {
		return driverId;
	}
	
	public String gettravelId() {
		return travelId;
	}

	public String getdateX() {
		return dateX;
	}

	public String gettimeX() {
		return timeX;
	}
	
	public String getlat() {
		return lat;
	}

	public String getlon() {
		return lon;
	}

	public double getspeed() {
		return speed;
	}

	public String geteventType() {
		return eventType;
	}

}
