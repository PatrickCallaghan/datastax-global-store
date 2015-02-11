create keyspace if not exists datastax_global_store WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

use datastax_global_store; 

CREATE TABLE if not exists object ( 
	key text,
	value text,
	PRIMARY KEY (key)
);

CREATE TABLE if not exists timeseries ( 
	key text,
	date timestamp,
	value double,
	PRIMARY KEY (key, date)
) with clustering order by (date desc);

CREATE TABLE if not exists timeseries_full ( 
	key text,
	dates blob,
	values blob,
	PRIMARY KEY (key)
);

CREATE TABLE if not exists datapoints ( 
	key text,
	name text,
	value double,
	PRIMARY KEY (key, name)
);

CREATE TABLE if not exists service_usage (
	namespace text,
	key text,	
	service text,
	ddmmyyyy text,
	count counter,
	PRIMARY KEY (namespace, key, service, ddmmyyyy)
);	
	
