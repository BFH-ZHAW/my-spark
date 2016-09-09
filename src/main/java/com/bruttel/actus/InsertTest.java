package com.bruttel.actus;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class InsertTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    SparkSession sparkSession = SparkSession
	    		.builder()
	    		.appName("sparkjobs.MapContractsToEventsJob")
	 //Funktioniert leider nicht, weil die Hive Klassen nicht gefunden wurden.  
	    		.enableHiveSupport()
	    		.getOrCreate();
	    System.out.println("ANFANG");
	    
	    sparkSession.sql("CREATE TABLE IF NOT EXISTS eventsSQL (id INT, datum STRING);");
	    sparkSession.sql("INSERT INTO TABLE eventsSQL VALUES (1, '09.09.2016');");
	    Dataset<Row> dfSQL = sparkSession.sql("SELECT * FROM eventsSQL;");
	    dfSQL.schema();
	    dfSQL.show();
	    dfSQL.write().mode("append").csv("s3a://bfh-zhaw/output/sql.csv");
	    
	    System.out.println("ENDE");
	    
	    // stop spark context
	    sparkSession.stop();
	    

	}

}
