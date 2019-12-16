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
	/*
	Regardless of the logging framework in use (log4j, logback, commons-logging, java.util.logging, etc.), loggers should be:
	- private: never be accessible outside of its parent class. If another class needs to log something, it should instantiate its own logger.
	- static: not be dependent on an instance of a class (an object). When logging something, contextual information can of course be provided in the messages but the logger should be created at class level to prevent creating a logger along with each object.
	- final: be created once and only once per class.
	*/
	
	public static void main(String[] args) throws Exception {
		//create a SparkConf object which will load values from any spark.* Java system properties set in our application
		SparkConf conf = new SparkConf()
			.setAppName(args[3]) /* app.name = CVS Data Processor */
			.setMaster(args[4]) /* spark.master = local[*] or spark://${SPARK_MASTER_IP}:7077 */
			.set("spark.cassandra.connection.host", args[5]) /* cassandra.host = 127.0.0.1 */ //configuration details of the Cassandra database server
			.set("spark.cassandra.connection.port", args[6]) /* cassandra.port = 9042 */ //configuration details of the Cassandra database server
			.set("spark.cassandra.connection.keep_alive_ms", args[7]); /* cassandra.keep_alive = 10000 */ //configuration details of the Cassandra database server
			// The connection to Cassandra will be closed shortly after all the tasks requiring Cassandra connectivity terminate. The period of time for keeping unused connections open is controlled by "spark.cassandra.connection.keep_alive_ms"
		
		//batch interval of 5 seconds for incoming stream. It means that our application collects streaming data in batch of five seconds. 	 
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		 
		//set the checkpoint directory in Java Streaming context to periodically checkpoint the operations for master fault-tolerance. You can find all checkpoints in this folder.
		jssc.checkpoint(args[8]); /* checkpoint.dir = /tmp/checkpoint-streaming-data */
		 
		//"Map" is a collection of key-value pairs. It maps keys to values. Map is implemented by "HashMap". HashMap is the object. HashMap cannot contain duplicate keys. HashMap allows null values and the null key. HashMap is an unordered collection. HashMap is not thread-safe. It means that you must explicitly synchronize concurrent modifications to the HashMap.
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("zookeeper.connect", args[0]); /* zookeeper = localhost:2181 */ //configuration details of ZooKeeper
		kafkaParams.put("metadata.broker.list", args[1]); /* brokerlist = localhost:9092 */ //configuration details of the Kafka broker. Comma is used as a separator, if there would be more than one broker.
		// a topic is where data is pulled from by the consumer.
		// CVSDataProcessor consumes all message from this topic.
		String topic = args[2]; /* topic.name = cvs-data-event */
		//"Set" represents a collection of objects where each object in the Set is unique. In other words, the same object cannot occur more than once in a Java Set. Set is implemented by "HashSet". The main different from a "List" where each element can occur more than once.
		Set<String> SetOfTopics = new HashSet<String>();
		SetOfTopics.add(topic);
		
		//read the direct kafka stream
		//create an input stream that directly pulls messages from Kafka Broker
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
		//tuple._1 contains topic name
		JavaDStream<CVSData> CVSDataStream = directKafkaStream.map(tuple -> tuple._2());
		 
		//map Cassandra travels_info table columns
		Map<String, String> columnNameMappings = new HashMap<String, String>();
			columnNameMappings.put("driverId", "driverId");
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
