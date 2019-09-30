package com.cvs.app.spark.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------

public class CVSDataDecoder implements Decoder<CVSData> {
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	public CVSDataDecoder(VerifiableProperties verifiableProperties) {

    }
	
	/*
	 * Method to deserialise CVSData objects.
	 */
	public CVSData fromBytes(byte[] bytes) {
		try {
			return objectMapper.readValue(bytes, CVSData.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
