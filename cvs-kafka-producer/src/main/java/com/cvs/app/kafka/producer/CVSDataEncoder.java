package com.cvs.app.kafka.producer;

import org.apache.log4j.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------

public class CVSDataEncoder implements Encoder<CVSData> {
	
	private static final Logger logger = Logger.getLogger(CVSDataEncoder.class);	
	private static ObjectMapper objectMapper = new ObjectMapper();		
	
	public CVSDataEncoder(VerifiableProperties verifiableProperties) {

    }
	
	/*
	 * Method to serialise CVSData objects.
	 */
	public byte[] toBytes(CVSData cvsEvent_toBytes) {
		try {
			String msg = objectMapper.writeValueAsString(cvsEvent_toBytes);
			logger.info(msg);
			return msg.getBytes();
		} catch (JsonProcessingException e) {
			logger.error("Error in Serialisation", e);
		}
		return null;
	}

}
