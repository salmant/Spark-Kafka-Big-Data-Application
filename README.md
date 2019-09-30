# cvs-big-data-application

The CVS Big Data application is designed and implemented upon a scalable microservices architecture providing different functionalities:
(1) Real-time streaming data analytics and visualisation
(2) Vehicle tracking e.g. GPS, mapping, navigation, etc. using HERE.com and Google Map APIs
(3) Driver behaviour profiling using AI and Fuzzy Logic

The CVS Big Data application includes 6 container-based components deployable on cloud, fog and edge computing infrastructures:
(1) Kafka Producer (Edge Processing) on Raspberry Pi
(2) Kafka Message Broker
(3) Spark Processor (Streaming Analytics) on Azure
(4) Cassandra TSDB (Database Server) on Amazon AWS
(5) RESTful Server (Java Servlet APIs) 
(6) GUI Server (Apache Server + PHP, JavaScript, CSS, etc.)

![Image](https://media-exp1.licdn.com/media-proxy/ext?w=800&h=800&f=n&hash=krvwP1aMMYtviCDAN7CMw4%2FFre4%3D&ora=1%2CaFBCTXdkRmpGL2lvQUFBPQ%2CxAVta5g-0R6jnhodx1Ey9KGTqAGj6E5DQJHUA3L0CHH05IbfPWjtLMWKeLCm8UAWeHkAjQA1Kuy1EmPmFo68KonvfIh2gpHmLMH5agYUbhl4lWdI)


