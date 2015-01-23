package com.datastax.demo;


public class SchemaSetupMulti extends SchemaSetup {

	public void setUp() {
		DROP_KEYSPACE = "DROP KEYSPACE order_management";
		CREATE_KEYSPACE = "CREATE KEYSPACE order_management WITH replication = "
				+ "{'class' : 'NetworkTopologyStrategy', 'Cassandra' : 3, 'Analytics' : 1, 'Solr' : 1}";
		
		logger.info ("Starting Multi Center DSE setup.");

		this.internalSetup();
		logger.info ("Finished Multi Center DSE setup.");
	}


	public static void main(String args[]) {

		SchemaSetupMulti setup = new SchemaSetupMulti();
		setup.setUp();
		setup.shutdown();
	}
}
