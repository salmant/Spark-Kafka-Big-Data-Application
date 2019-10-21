// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - CVS Mobile Company and Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------


CREATE KEYSPACE IF NOT EXISTS cvsdatabase  
	WITH REPLICATION = { 
		'class' : 'SimpleStrategy', 
		'datacenter1' : 1 
	} ;

CREATE TABLE IF NOT EXISTS cvsdatabase.totalbased_info (
	travelId text, 
	eventType text, 
	totalCount bigint, 
	timeStamp timestamp, 
	recordDate text, 
	PRIMARY KEY (travelId, totalCount, recordDate)
);

CREATE TABLE IF NOT EXISTS cvsdatabase.windowbased_info (
	travelId text, 
	eventType text, 
	totalCount bigint, 
	timeStamp timestamp, 
	recordDate text, 
	PRIMARY KEY (travelId, totalCount, recordDate)
);

CREATE TABLE IF NOT EXISTS cvsdatabase.travels_info (
	driverId text, 
	travelId text, 
	dateX text, 
	timeX text, 
	lat text, 
	lon text, 
	speed double, 
	eventType text, 
	PRIMARY KEY (driverId, travelId, dateX, timeX)
);

CREATE INDEX ON cvsdatabase.travels_info (travelid);

CREATE INDEX ON cvsdatabase.totalbased_info (eventtype);

CREATE INDEX ON cvsdatabase.windowbased_info (eventtype);

