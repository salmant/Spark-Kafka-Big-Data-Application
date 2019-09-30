package com.cvs.app.spark.processor;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.base.Optional;
import scala.Tuple2;

// ------------------------------------------------------------------------
// Author: Salman Taherizadeh - Jozef Stefan Institute (JSI)
// This code is published under the Apache 2 license
// ------------------------------------------------------------------------

public class CVSTrafficDataProcessor {
	
	private static final Logger logger = Logger.getLogger(CVSTrafficDataProcessor.class);

	/*
	 * Method to calculate the total number of each type of driving dynamics for each travel.
	 */
	public void processTotalDataStream(JavaDStream<CVSData> CVSDataStream_as_JavaDStream) {

		//this part is a transformation to process total count for different types of driving dynamics for each travel.
		//calculate the total number of driving dynamic events grouped by travelId and eventType.
		//therefore we defined the ProcessKey class which includes these two attributes: travelId and eventType
		//ProcessKey object is a key in countDStreamPair defined as JavaPairDStream<ProcessKey, Long>.
		//We use mapToPair transformation for each count and reduceByKey to combine the same ProcessKey in pair if any. 
		JavaPairDStream<ProcessKey, Long> countDStreamPair = CVSDataStream_as_JavaDStream
				.mapToPair(iot -> new Tuple2<>(new ProcessKey(iot.gettravelId(), iot.geteventType()), 1L))
				.reduceByKey((a, b) -> a + b);
		
		//keep the state for total count
		JavaMapWithStateDStream<ProcessKey, Long, Long, Tuple2<ProcessKey, Long>> countDStreamWithStatePair = countDStreamPair
				.mapWithState(StateSpec.function(totalSumFunc).timeout(Durations.seconds(3600))); //maintain state for one hour

		//transform to JavaDStream by totalBasedInfoFunc
		//this function will transform the JavaDstream from previous transformation to JavaDStream<TotalTrafficData>
		JavaDStream<Tuple2<ProcessKey, Long>> countDStream = countDStreamWithStatePair.map(tuple2 -> tuple2);
		JavaDStream<TotalBasedInfo> trafficDStream = countDStream.map(totalBasedInfoFunc);

		//map Cassandra totalbased_info table columns - column mapping
		Map<String, String> columnNameMappings = new HashMap<String, String>();
		columnNameMappings.put("travelId", "travelid");
		columnNameMappings.put("eventType", "eventtype");
		columnNameMappings.put("totalCount", "totalcount");
		columnNameMappings.put("timeStamp", "timestamp");
		columnNameMappings.put("recordDate", "recorddate");

		//call this function to save the calculated information in Cassandra database
		//it uses the Spark Cassandra connector library from datastax to provide API to save DStream or RDD in Cassandra
		//therfore, this information saved in Cassandra can be fetched by any other third party such as a gui.
		javaFunctions(trafficDStream).writerBuilder("cvsdatabase", "totalbased_info",
				CassandraJavaUtil.mapToRow(TotalBasedInfo.class, columnNameMappings)).saveToCassandra();
		
	}

	/*
	 * Method to calculate the number of each type of driving dynamics for each travel happend in the last 60 seconds.
	 * In another words, calculating the window traffic counts of different types of driving dynamics for each travel in the last 60 seconds 
	 * Window Duration = 60 seconds and Slide Interval = 10 seconds
	 */
	public void processWindowDataStream(JavaDStream<CVSData> CVSDataStream_as_JavaDStream) {

		/* 
		 * calculate the number of driving dynamic events grouped by travelId and eventType happend in the last 60 seconds.
		 * reduced by key and window (60-second window and 10-sececond slide).
		 * this part uses the Spark window-based API.
		 * we do not maintain any state for teh window-based count.
		*/
		JavaPairDStream<ProcessKey, Long> countDStreamPair = CVSDataStream_as_JavaDStream
				.mapToPair(iot -> new Tuple2<>(new ProcessKey(iot.gettravelId(), iot.geteventType()), 1L))
				.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(60), Durations.seconds(10));

		//transform to JavaDStream by windowBasedInfoFunc
		//this function will transform the JavaDstream from previous transformation to JavaDStream<WindowBasedInfo>
		JavaDStream<WindowBasedInfo> trafficDStream = countDStreamPair.map(windowBasedInfoFunc);

		//map Cassandra windowbased_info table columns - column mapping
		Map<String, String> columnNameMappings = new HashMap<String, String>();
		columnNameMappings.put("travelId", "travelid");
		columnNameMappings.put("eventType", "eventtype");
		columnNameMappings.put("totalCount", "totalcount");
		columnNameMappings.put("timeStamp", "timestamp");
		columnNameMappings.put("recordDate", "recorddate");
		
		//call this function to save the calculated information in Cassandra database
		//it uses the Spark Cassandra connector library from datastax to provide API to save DStream or RDD in Cassandra
		//therfore, this information saved in Cassandra can be fetched by any other third party such as a gui.
		javaFunctions(trafficDStream).writerBuilder("cvsdatabase", "windowbased_info",
				CassandraJavaUtil.mapToRow(WindowBasedInfo.class, columnNameMappings)).saveToCassandra();
		
	}

	//Function to calculate running sum by maintaining the state
	private static final Function3<ProcessKey, Optional<Long>, State<Long>,Tuple2<ProcessKey, Long>> totalSumFunc = (key, currentSum, state) -> {
        long totalSum = currentSum.or(0L) + (state.exists() ? state.get() : 0);
        Tuple2<ProcessKey, Long> total = new Tuple2<>(key, totalSum);
        state.update(totalSum);
        return total;
    };
    
    //Function to create TotalBasedInfo object
    private static final Function<Tuple2<ProcessKey, Long>, TotalBasedInfo> totalBasedInfoFunc = (tuple -> {
		logger.debug("Total Count Calculation: " + "Key = " + tuple._1().gettravelId() + " - " + tuple._1().geteventType() + " Value = " + tuple._2());
		TotalBasedInfo trafficData = new TotalBasedInfo();
		trafficData.settravelId(tuple._1().gettravelId());
		trafficData.seteventType(tuple._1().geteventType());
		trafficData.setTotalCount(tuple._2());
		trafficData.setTimeStamp(new Date());
		trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		return trafficData;
	});
    
    //Function to create WindowBasedInfo object
    private static final Function<Tuple2<ProcessKey, Long>, WindowBasedInfo> windowBasedInfoFunc = (tuple -> {
		logger.debug("Window Count Calculation: " + "Key = " + tuple._1().gettravelId() + " - " + tuple._1().geteventType()+ " Value = " + tuple._2());
		WindowBasedInfo trafficData = new WindowBasedInfo();
		trafficData.settravelId(tuple._1().gettravelId());
		trafficData.seteventType(tuple._1().geteventType());
		trafficData.setTotalCount(tuple._2());
		trafficData.setTimeStamp(new Date());
		trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		return trafficData;
	});
    
}
