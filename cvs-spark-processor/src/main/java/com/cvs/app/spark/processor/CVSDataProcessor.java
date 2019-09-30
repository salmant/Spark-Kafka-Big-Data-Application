package com.cvs.app.spark.processor;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import com.google.common.base.Optional;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------

/*
 * Class to consume CVS data messages from Kafka using Spark APIs, generates the stream for processing the data and sinks it to the target Cassandra database.
 */
public class CVSDataProcessor {
	
	private static final Logger logger = Logger.getLogger(CVSDataProcessor.class);
	
	public static void main(String[] args) throws Exception {
		//create a SparkConf object which will load values from any spark.* Java system properties set in our application
		SparkConf conf = new SparkConf()
			.setAppName(args[3]) /* app.name = CVS Data Processor */
			.setMaster(args[4]) /* spark.master = local[*] */
			.set("spark.cassandra.connection.host", args[5]) /* cassandra.host = 127.0.0.1 */ //configuration details of the Cassandra database server
			.set("spark.cassandra.connection.port", args[6]) /* cassandra.port = 9042 */ //configuration details of the Cassandra database server
			.set("spark.cassandra.connection.keep_alive_ms", args[7]); /* cassandra.keep_alive = 10000 */ //configuration details of the Cassandra database server
		 
		//batch interval of 5 seconds for incoming stream. It means that our application collects streaming data in batch of five seconds. 	 
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		 
		//set the checkpoint directory in Java Streaming context. You can find all checkpoints in this folder.
		jssc.checkpoint(args[8]); /* checkpoint.dir = /tmp/checkpoint-streaming-data */
		 
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("zookeeper.connect", args[0]); /* zookeeper = localhost:2181 */ //configuration details of ZooKeeper
		kafkaParams.put("metadata.broker.list", args[1]); /* brokerlist = localhost:9092 */ //configuration details of the Kafka broker
		// a topic is where data is pulled from by the consumer.
		// CVSDataProcessor consumes all message from this topic.
		String topic = args[2]; /* topic.name = cvs-data-event */
		Set<String> SetOfTopics = new HashSet<String>();
		SetOfTopics.add(topic);
		
		//read the direct kafka stream
		JavaPairInputDStream<String, CVSData> directKafkaStream = KafkaUtils.createDirectStream(
			        jssc,
			        String.class,
			        CVSData.class,
			        StringDecoder.class,
			        CVSDataDecoder.class,
			        kafkaParams,
			        SetOfTopics
			    );
				
		logger.info("\n ######### Starting the Spark Stream Processing ######### \n");
		 
		//the map operation to get CVSData objects and transform it to JavaDStream
		JavaDStream<CVSData> CVSDataStream = directKafkaStream.map(tuple -> tuple._2());
		 
		//map Cassandra travels_info table columns
		Map<String, String> columnNameMappings = new HashMap<String, String>();
			columnNameMappings.put("vehicleId", "vehicleid");
			columnNameMappings.put("travelId", "travelid");
			columnNameMappings.put("dateX", "datex");
			columnNameMappings.put("timeX", "timex");
			columnNameMappings.put("lat", "lat");
			columnNameMappings.put("lon", "lon");
			columnNameMappings.put("speed", "speed");
			columnNameMappings.put("eventType", "eventtype");
			
		//call this function to save received messages in Cassandra database
		javaFunctions(CVSDataStream).writerBuilder("cvsdatabase", "travels_info",
			CassandraJavaUtil.mapToRow(CVSData.class, columnNameMappings)).saveToCassandra();
		
		JavaDStream<CVSData> CVSDataStream_as_JavaDStream = CVSDataStream;
		
		//cache the stream because it will be used in both total and window based processing jobs
		CVSDataStream_as_JavaDStream.cache();
		 
		//there are two jobs to process stream. 
		CVSTrafficDataProcessor cvsTrafficProcessor = new CVSTrafficDataProcessor();
		//the first job for calculating the total number of each type of driving dynamics for each travel.
		cvsTrafficProcessor.processTotalDataStream(CVSDataStream_as_JavaDStream);
		//the second job for calculating the number of each type of driving dynamics for each travel happend in the last 60 seconds.
		cvsTrafficProcessor.processWindowDataStream(CVSDataStream_as_JavaDStream);
		
		//start context
		jssc.start();            
		jssc.awaitTermination();  
	}
	
}
