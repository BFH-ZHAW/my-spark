/**
Berechnet den Nominal Value Aller Events
Datenverarbeitung: Dataframe
Output: Liste
Input: parquet
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.*;


public class CallculateNominalValue_V1_0 {

	public static void main(String[] args) {
		if (args.length != 7) {
			System.err.println("Usage: path logFile output debug ram run knoten");
			System.exit(0);
		}
		// Input Parameter:
		String path = args[0]; // hdfs://160.85.30.40/user/spark/data/
		String logFile = args[1]; // logX.csv (Name des Logfiles -> wird f�rs Log Gebraucht)
		String output = args[2]; // parquet oder CSV
		String debug = args[3]; // write debug to debug or anything else to not debug
		String ram = args[4]; // 12GB (Ram Pro Executor -> wird f�rs LogGebraucht)
		String run = args[5]; // 1-10 (Durchgang -> wird f�rs Log gebraucht)
		String knoten = args[6]; // 1-8 (Anzahl aktive Knoten -> wird f�rs Log gebraucht)

		// Pfade
		String outputPath = path.concat("output/"); // Kompletter Pfad zum Output Path


		// Klassenname wird wieder verwendet:
		String className = "com.bruttel.actus.CallculateNominalValue_V1_0";

		// Create Spark Session
		SparkSession sparkSession = SparkSession.builder().appName(className).getOrCreate();

		// for time stopping
		long start = System.currentTimeMillis();

		// import Events
		Dataset<Row> events =  sparkSession.read().parquet(outputPath.concat("events.parquet"));
 
		//Register as table to use sql
		events.createOrReplaceTempView("events");
		
		// Debug Info
		if (debug.equals("debug")) {
	    	System.out.println("events geladen");
	    	events.printSchema();
	    	events.show();
		}
		
		// Aggregate Values
		Dataset<Row> nominalValues = events.filter("type = 'AD0'")
										   .groupBy("riskScenario", "portfolio")
										   .agg(sum(expr("nominal/1000000")).alias("summe"))
										   .cache();
		// Debug Info
		if (debug.equals("debug")) {
	    	System.out.println("events nominalValues");
	    	nominalValues.printSchema();
	    	nominalValues.show();
		}
		Dataset<Row> nominalValuesMean = nominalValues.groupBy("portfolio")
													  .agg(mean("summe").alias("Mittelwert"), 
														   max("summe").alias("Maximum"),  
														   min("summe").alias("Minimum"))
													  .orderBy("portfolio");

		// Debug Info
		if  (debug.equals("debug")) { 
			System.out.println("events nominalValuesMean");
	    	nominalValuesMean.printSchema();
	    	nominalValuesMean.show();
	    }
		
		//Output generieren:
	    if(output.equals("parquet")){
	    	nominalValuesMean.write().mode(SaveMode.Overwrite).parquet(outputPath + "nominalValues.parquet");
	    }
	    else {
	    	nominalValuesMean.write().mode(SaveMode.Overwrite).csv(outputPath + "nominalValues.csv");
	    }

		// Ende der Zeitmessung:
		long stop = System.currentTimeMillis();

		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		//new WriteLog(logFile, className, output, ram, contracts, riskfactors, knoten, run, start, stop);
		new WriteLog(logFile, className, output, ram, "", "", knoten, run, start, stop);

	}

}