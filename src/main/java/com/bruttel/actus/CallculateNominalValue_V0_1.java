/**
Berechnet den Nominal Value Aller Events
Datenverarbeitung: SQL -> Nested
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

public class CallculateNominalValue_V0_1 {

	public static void main(String[] args) {
		if (args.length != 7) {
			System.err.println("Usage: path logFile output debug ram run knoten");
			System.exit(0);
		}
		// Input Parameter:
		String path = args[0]; // hdfs://160.85.30.40/user/spark/data/
		String logFile = args[1]; // logX.csv (Name des Logfiles -> wird fürs Log Gebraucht)
		String output = args[2]; // parquet oder CSV
		String debug = args[3]; // write debug to debug or anything else to not debug
		String ram = args[4]; // 12GB (Ram Pro Executor -> wird fürs LogGebraucht)
		String run = args[5]; // 1-10 (Durchgang -> wird fürs Log gebraucht)
		String knoten = args[6]; // 1-8 (Anzahl aktive Knoten -> wird fürs Log gebraucht)

		// Pfade
		String outputPath = path.concat("output/"); // Kompletter Pfad zum Output Path


		// Klassenname wird wieder verwendet:
		String className = "com.bruttel.actus.CallculateNominalValue_V0_1";

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

		//SQL query with no sucess to get no files back
		String sqlquery = "SELECT portfolio, mean(NominalInMio) as MeanNominalInMio, max(NominalInMio) as MaxNominalinMio, min(NominalInMio) as MinNominalinMio "
				+ "FROM (SELECT riskScenario, portfolio, sum(nominal) / 1000000 as NominalInMio "
				+ "		FROM events "
				+ "		WHERE type = 'AD0' "
				+ "		GROUP BY riskScenario, portfolio) AS table "
				+ "GROUP BY  portfolio "
				+ "ORDER BY portfolio";
		
		//Execute SQL
		Dataset<Row> nominalValuesMean = sparkSession.sql(sqlquery).cache();
		
		// Debug Info
		if (debug.equals("debug")) {
	    	System.out.println("nominalValuesMean");
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
		nominalValuesMean.unpersist(true);
		sparkSession.sqlContext().dropTempTable("events");
		sparkSession.stop();

		// Log Schreiben
		//new WriteLog(logFile, className, output, ram, contracts, riskfactors, knoten, run, start, stop);
		new WriteLog(logFile, className, output, ram, "", "", knoten, run, start, stop);

	}

}