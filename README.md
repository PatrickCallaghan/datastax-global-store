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

###Service Usage
There is also a table called 'service_usage' which shows the number of reads and writes for each type of dataset for each namespace and key.
   
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

###Post an object to the store

curl -X POST -H "Content-Type: application/json" -d'{"key":"FX/test","value":"rO0ABXNyAA5qYXZhLnV0aWwuRGF0ZWhqgQFLWXQZAwAAeHB3CAAAAUsmhmEjeA"}' http://localhost:8080/datastax-global-store/rest/post/object

###Get object from the store

curl -X GET -H "Content-Type: application/json" http://localhost:8080/datastax-global-store/rest/get/object?key=FX/test

###Post new items to a time series

curl -X POST -H "Content-Type: application/json" -d'{"key":"FX/test","dates":[1395792120000], "values":[102] }' http://localhost:8080/datastax-global-store/rest/post/timeseries

curl -X POST -H "Content-Type: application/json" -d'{"key":"FX/test","dates":[1395793120000], "values":[110] }' http://localhost:8080/datastax-global-store/rest/post/timeseries

###Read a times series back
 
curl -X GET -H "Content-Type: application/json" http://localhost:8080/datastax-global-store/rest/get/timeseries?key=FX/test

###Post full time series data

curl -X POST -H "Content-Type: application/json" -d'{"key":"FX/test","dates":[1395792120000,1395883380000,1395883440000,1395885840000,1395886320000,1395886980000,1395887040000,1395889140000,1395889260000,1395889380000,1395890100000,1395890760000,1395891240000,1395891540000,1395893520000,1395894180000,1395894300000,1395894360000,1395895140000,1395895740000,1395895920000,1395896340000,1395896580000,1395896640000,1395898440000,1395898920000,1395898980000,1395899040000,1395899640000,1395899700000,1395899820000,1395899940000,1395900060000,1395900240000,1395900600000,1395900780000,1395900960000,1395901200000,1395901560000,1395901740000,1395902280000,1395902460000,1395902520000,1395902760000,1395902940000],"values":[5.0,6.0,4.0,3.0,4.0,5.0,6.0,7.0,5.0,5.0,6.0,7.0,8.0,5.0,5.0,6.0,7.0,8.0,9.0,7.0,6.0,5.0,6.0,4.0,3.0,5.0,6.0,7.0,5.0,4.0,6.0,3.0,4.0,5.0,7.0,5.0,4.0,6.0,7.0,5.0,5.0,4.0,6.0,7.0,8.0]}' http://localhost:8080/datastax-global-store/rest/post/timeseriesfull

###Read the time series back

curl -X GET -H "Content-Type: application/json"  http://localhost:8080/datastax-global-store/rest/get/timeseriesfull?key=FX/test

###Posts Data Points 

curl -X POST -H "Content-Type: application/json" -d '{"key":"FX/test","names":["London", "Santa Clara", "New York"],"values":[5.0,6.0,4.0]}' http://localhost:8080/datastax-global-store/rest/post/datapoints

###Read the data points back

curl -X GET -H "Content-Type: application/json" http://localhost:8080/datastax-global-store/rest/get/datapoints?key=FX/test



