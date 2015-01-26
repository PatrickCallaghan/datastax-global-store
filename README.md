Global Data Store
========================================================

This is a simple example of using C* as a Global data store for purely storing data for high availability. The global store allows data to be replicated in different datacenters geographically distributed around the globe. It also provides this through a simple rest api to allow the data to accessed from any location.  

## Running the demo 

You will need a java runtime (preferably 7) along with maven 3 to run this demo. Start DSE 3.1.X or a cassandra 1.2.X instance on your local machine. This demo just runs as a standalone process on the localhost.

## Use Cases

###Put/Get Cache
This allow a client to store any type of data which has been encoded into a String. This is mainly used for base 64 encoding and provides a simple way to transfer bytes.

###Time Series
The global store allows 2 types of times series data to be held.

1. Expanding dataset - this is used when the time series grows over a period of time e.g sensor data, finanical ticks

2. Static Set - a time series which is result of a computation which will be completely overridden in the future e.g. forward rates for a particular data, cash flows. 

###Data Point
This is data where there is a string-number relationship e.g. Temperatures - London:76, New York:45, Santa Clara:82

## Schema Setup
Note : This will drop the keyspace "datastax_global_store" and create a new one. All existing data will be lost. 

The schema can be found in src/main/resources/cql/

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run the web server 

    mvn jetty:run
    
This will start the server on the localhost at port 8080.

To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
	
##Using

Curl  
