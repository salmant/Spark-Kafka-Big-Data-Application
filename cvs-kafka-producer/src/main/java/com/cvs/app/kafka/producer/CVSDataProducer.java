package com.cvs.app.kafka.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.apache.log4j.Logger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------

/*
 * Class to receive vehicles's measured parameters and produce events called the CVS data messages using the Kafka producer.
 */
public class CVSDataProducer {
	
	private static final Logger logger = Logger.getLogger(CVSDataProducer.class);
	private static String driverId;

	public static void main(String[] args) throws Exception {
		
		driverId = args [3]; // e.g. "65440984"	
		String zookeeper = args [0]; /* zookeeper = localhost:2181 */ //configuration details of ZooKeeper
		String broker = args [1]; /* brokerlist = localhost:9092 */ //configuration details of the Kafka broker
		// a topic is where data (messages) gets published to by the producer.
		// CVSDataProducer produces all messages on this topic.
		String topic = args [2]; /* topic.name = cvs-data-event */
		logger.info("\n ######### Zookeeper =  " + zookeeper + ", Broker = " + broker + " and Topic = " + topic + " ######### \n");

		//set producer properties
		Properties properties = new Properties();
		properties.put("zookeeper.connect", zookeeper);
		properties.put("metadata.broker.list", broker);
		properties.put("request.required.acks", "1");
		properties.put("serializer.class", "com.cvs.app.kafka.producer.CVSDataEncoder");
		
		//generate event
		Producer<String, CVSData> producer = new Producer<String, CVSData>(new ProducerConfig(properties));
		CVSDataProducer cvsProducer = new CVSDataProducer();
		cvsProducer.generateCVSDataMessages(producer,topic);		
	}


	/*
	 * Method running in an infinite while loop to generate the data messages. 
	 * For instance: {"driverId":"65440984","travelId":"b2d63d93-d3f0-4139-acba-1331356b6e9c","dateX":"29/09/2019","timeX":"19:21:04","lat":"46.056946","lon":"14.505751","speed":57.0,"eventType":"GPS"}
	 */
	private void generateCVSDataMessages(Producer<String, CVSData> producer, String topic) throws InterruptedException {
		List<String> eventTypeList = Arrays.asList(new String[]{"GPS", "Aggressive Right", "Aggressive Left", "Sudden Acceleration", "Hard Braking"});
		
		Random rand = new Random();
		
		//each time a vehicle starts up, a new ID will be generated to be assigned to the travel
		String travelId = UUID.randomUUID().toString(); 
		
		while (true) {
			Date date_time = new Date();
			SimpleDateFormat dateFormat1 = new SimpleDateFormat("dd/MM/yyyy");
			String dateX = String.valueOf(dateFormat1.format(date_time));
			SimpleDateFormat dateFormat2 = new SimpleDateFormat("HH:mm:ss");
			String timeX = String.valueOf(dateFormat2.format(date_time));
			
			String lat = "46.056946"; //it should be taken from the GPS sensor
			String lon = "14.505751"; //it should be taken from the GPS sensor
			double speed = rand.nextInt(150); //take a random speed between 0 to 150 - it should be taken from the vehicle velocity sensor
			//there are five different types of driving dynamics which are "GPS", "Aggressive Right", "Aggressive Left", "Sudden Acceleration" or "Hard Braking".
			String eventType = eventTypeList.get(rand.nextInt(5)); //take a random eventType which could be "GPS", "Aggressive Right", "Aggressive Left", "Sudden Acceleration" or "Hard Braking"
			
			CVSData event = new CVSData(driverId, travelId, dateX, timeX, lat, lon, speed, eventType);
			KeyedMessage<String, CVSData> data = new KeyedMessage<String, CVSData>(topic, event);
			producer.send(data);
			
			Thread.sleep(5000); //it periodically sends a data message called event to the Kafka message broker every 5 seconds
		}
	}
}
