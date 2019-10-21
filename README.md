# Spark-Kafka-Big-Data-Application

This repository represents a small part of a Big Data project (shown below) implemented for the CVS Mobile Company.

![Image](https://media-exp1.licdn.com/media-proxy/ext?w=800&h=800&f=n&hash=tYIJMhoInxnwEzcGas%2F48XQbX6o%3D&ora=1%2CaFBCTXdkRmpGL2lvQUFBPQ%2CxAVta5g-0R6jnhodx1Ey9KGTqAGj6E5DQJHUA3L0CHH05IbfPWjpLZTfLbr3p0ASfXgAjQBkK-i1SDm3RI7pK47sfo91g8WxJMT5agYUbhl4lWdI)

The CVS Big Data project is designed and implemented upon a zero-downtime, scalable microservices architecture providing different functionalities:
* Streaming analytics in real-time, batch processing and visualisation dashboard.
* Vehicle tracking e.g. direction, GPS, mapping, navigation, etc. using HERE.com and Google Map APIs.
* Driver behaviour profiling using AI and Fuzzy Logic.

The CVS Big Data application includes various components deployable on cloud, fog and edge computing infrastructures:
* Kafka Producer: `cvs-kafka-producer` is the edge processing part of the application deployed on Raspberry Pi.
* Kafka Broker: It is a widely used distributed streaming platform capable of handling trillions of events a day for messages passed within the system. This component can be deployed on the fog or cloud computing infrastructures.
* Spark Processor: `cvs-spark-processor` is a powerful streaming analytics tool deployed on the cloud.
* Database Server: It is implemented by the Apache Cassandra time series database. In order to create the Cassandra database, this file can be used: 
* etc.

**Kafka broker**: Apache Kafka broker is an open-source distributed streaming platform developed to provide a unified, high-throughput, low-latency broker for handling real-time data feeds. Kafka can connect to external systems for data import/export via Kafka Connect and provides Kafka Streams. Kafka allows applications publish and subscribe to real-time streams of records called `topics`. Kafka is basically used to build real-time streaming Big Data applications or data pipelines.
<br><br>
**Spark streaming analytics engine**: Apache Spark is an open-source distributed general-purpose cluster-computing framework. Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. Resilient Distributed Datasets called `RDD` is a fundamental data structure of Spark. It is an immutable distributed collection of objects. RDDs may be operated in parallel across a cluster of computing nodes called Spark Workers. To operate in parallel, RDDs are divided into logical partitions. Therefore, partitions are computed on multiple cluster nodes (Spark Workers). 
<br><br>
**Apache Cassandra**: Cassandra TSDB, usable in many scaling scenarios, is a free, open-source, column-oriented, NoSQL database system exploited to store the time series data. Cassandra database is designed to store and handle large amount of data. It has its own query language called `CQL` (Cassandra Query Language). 
<br><br>
The `cvs-kafka-producer` running on the edge side receives data from vehicle sensors and recognises different types of unexpected driving dynamics (such as `sudden acceleration`, `hard braking`, `aggressive right turn` and `aggressive left turn`). If there would be any driving dynamics, it instantly sends a run-time message to the Kafka Broker. Moreover, the `cvs-kafka-producer` periodically transmits the `GPS` information that is helpful to know where the vehicle is located or moving, etc. This information will be stored in a Cassandra table named `cvsdatabase.travels_info`. Therefore, messages sent to the Kafka Broker include different fields: [CVSData.java](https://github.com/salmant/Spark-Kafka-Big-Data-Application/blob/master/cvs-kafka-producer/src/main/java/com/cvs/app/kafka/producer/CVSData.java)
<br>
*  `driverId`: Each driver has a unique id. 
*  `travelId`: Each time a vehicle starts up, a new ID will be generated to be assigned to the travel.
*  `dateX`: The data on which the data message is sent.
*  `timeX`: The time implying when the data message is sent.
*  `lat`: GPS latitude
*  `lon`: GPS longitude
*  `speed`: The speed of the vehicle.
*  `eventType`: The type of message event could be GPS, Aggressive Right, Aggressive Left, Sudden Acceleration or Hard Braking.

The Kafka broker receives all events sent by the `cvs-kafka-producer`. Afterwards, the Kafka broker forwards all events to the `cvs-spark-processor` which processes the streaming sensor data in real-time, extracts useful knowledge and sends the information to be stored in the Cassandra database. The logistic centre would like to know if any driver is dangerously maneuvering at the moment on situations when many dynamics (sudden acceleration, hard braking, aggressive right turn and aggressive left turn) are currently happening. To this end, the `cvs-spark-processor` provides the following real-time streaming analytics:

* Calculating the number of each type of dynamics (sudden acceleration, hard braking, aggressive right turn and aggressive left turn as well as GPS) for each travel happend in the last 60 seconds. This information will be stored in a Cassandra table named `cvsdatabase.windowbased_info`. 
* Calculating the total number of each type of dynamics (sudden acceleration, hard braking, aggressive right turn and aggressive left turn as well as GPS) from the beginning for each travel. This information will be stored in a Cassandra table named `cvsdatabase.totalbased_info`. 

Therefore, the logistic centre can easily find out how their drivers drive on the road, such as the rate of braking, turning and vehicle acceleration. For example, it should be noted that an unsafe driver performs hard acceleration, sudden braking and steering maneuvers more frequently than a safe and also moderate driver. It should be noted that such information can be used to generate whether a training system or an award system which may motivate drivers to keep trying to attain high standards of driving excellence.
<br><br>
NOTE: In order to proceed this guide, prior knowledge of working with the following technologies is highly required:

* Kafka 
* Spark 
* ZooKeeper
* Maven
* Cassandra time series database
* CQLSH
* Docker containers

Before you begin, make sure you have your own Kafka broker, ZooKeeper and Cassandra deployed and ready to be used. In order to instantiate the ZooKeeper service, you can execute the following command:<br><br>
`docker run -p 2181:2181 -p 2888:2888 -p 3888:3888 -d salmant/cvs_zookeeper_cloud:1.2`
<br><br>
In order to instantiate the Kafka broker, you can execute the following command:<br><br>
`docker run -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME="X.X.X.X" -e KAFKA_TOPIC="X-Y-Z" -e KAFKA_ZOOKEEPER_IP="Y.Y.Y.Y" -d salmant/cvs_kafka_broker_cloud:1.2`
<br><br>
As you can see, We need to define the values of three environment variables for the Kafka broker. The variable named `KAFKA_ZOOKEEPER_IP` is the IP address of the machine where the ZooKeeper service is running. The variable named `KAFKA_ADVERTISED_HOST_NAME` is the IP address of the machine where the Kafka broker itself is running. And the variable named `KAFKA_TOPIC` is the name of topic where events gets published to by the cvs-kafka-producer.
<br><br>


