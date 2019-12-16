package com.cvs.app.spark.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------

/*
 * Class to deserialise the CVS data messages.
 */

public class CVSDataDecoder implements Decoder<CVSData> {
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	//supplying a "VerifiableProperties" constructor is required
	public CVSDataDecoder(VerifiableProperties verifiableProperties) {

    }
	
	/*
	 * Method to deserialise CVSData objects.
	 */
	//"fromBytes(...)" method to read a byte array
	public CVSData fromBytes(byte[] bytes) {
		try {
			return objectMapper.readValue(bytes, CVSData.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
