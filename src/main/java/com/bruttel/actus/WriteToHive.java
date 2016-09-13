package com.bruttel.actus;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class WriteToHive {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// Create Spark Session	
		   SparkSession sparkSession = SparkSession
		    		.builder()
		    		.appName("com.bruttel.actus.CreateEmptyParquet")
		    		.enableHiveSupport()
		    		.getOrCreate();
//Insert in bestehende Tabelle
//		   System.out.println("SPARK SQL");
//		   sparkSession.sql("INSERT INTO TABLE test VALUES ('CT1',0.0)"); 
//		   sparkSession.sql("SELECT * FROM test").show();
//Neue Tabelle erstellen
		   System.out.println("SPARK SQL CREATE");
		   sparkSession.sql("CREATE TABLE IF NOT EXISTS testCreate (key INT, value STRING)");
		   sparkSession.sql("INSERT INTO TABLE testCreate VaLUES (1,'test'");
		   sparkSession.sql("SELECT * FROM testCreate").show();

	}

}
