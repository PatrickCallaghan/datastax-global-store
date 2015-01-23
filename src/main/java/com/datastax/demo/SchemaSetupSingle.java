package com.datastax.demo;


public class SchemaSetupSingle extends SchemaSetup {
		
	public void setUp(){
		
		DROP_KEYSPACE = "DROP KEYSPACE order_management";
		CREATE_KEYSPACE = "CREATE KEYSPACE order_management WITH replication = "
				+ "{'class' : 'SimpleStrategy', 'replication_factor' : 1}";
		
		logger.info ("Running Single Node DSE setup.");
		
		internalSetup();
		
		logger.info ("Finished Single Node DSE setup.");
	}



	public static void main(String args[]){
		
		SchemaSetupSingle setup = new SchemaSetupSingle();
		setup.setUp();
		setup.shutdown();
	}
}
