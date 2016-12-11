/**
Berechnet den Nominal Value Aller Events
Datenverarbeitung: SQL
Output: Liste
Input: parquet
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Arrays;
import java.util.List;

public class CallculateFundingLiq_V1_0 {

	public static void main(String[] args) {
		if (args.length < 7) {
			System.err.println("Usage: path logFile output debug ram run knoten zeitraum");
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
		String zeitraum = "0-1,1-2,2-5";
		if(args.length==8){
			zeitraum = args[7];  //1-2!2-5!5-6 (Verschiedene Zeitangaben, mit ! getrennt) -> Wird 
		}
		
		// Pfade
		String outputPath = path.concat("output/"); // Kompletter Pfad zum Output Path


		// Klassenname wird wieder verwendet:
		String className = "com.bruttel.actus.CallculateFundingLiq_V1_0";

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
		
		//Zeitr�ume definieren
		List<String> timeList = Arrays.asList(zeitraum.split(","));
		List<String> subTimeList;
		
		// Initialer Zeitraum
		subTimeList = Arrays.asList(timeList.get(0).split("-"));
		Dataset<Row> fundingLiquidity = events.filter("datediff(to_date(date), current_date())>  365*"+subTimeList.get(0)+" and "
													+ "datediff(to_date(date), current_date())<= 365*"+subTimeList.get(1))
										   	  .groupBy("riskScenario", "portfolio")
										      .agg(sum(expr("value/1000000")).alias("summe")); 
		// Debug Information
		if (debug.equals("debug")) {
	    	System.out.println("fundingLiquidity");
	    	fundingLiquidity.printSchema();
	    	fundingLiquidity.show();
	    	}
		
		// Initiale Zeitr�ume Aggregieren
		Dataset<Row> fundingLiquidity2 = fundingLiquidity.groupBy("portfolio")
													   .agg(mean("summe").alias("Mittelwert_"+timeList.get(0)),
													   		max("summe").alias("Maximum_"+timeList.get(0)),
													   		min("summe").alias("Minimum_"+timeList.get(0)))
													   .orderBy("portfolio");

		// Debug Info
		if (debug.equals("debug")) {
	    	System.out.println("fundingLiquidity");
	    	fundingLiquidity2.printSchema();
	    	fundingLiquidity2.show();
		}
		
		//Ieration pro weitere Zeitr�ume um diese hinzuzuf�gen
		for(int i=1; i<timeList.size(); i++){
			// n�chste von-bis Werte
			subTimeList = Arrays.asList(timeList.get(i).split("-"));

			// Initiale Zeitr�ume
			fundingLiquidity = events.filter("datediff(to_date(date), current_date())> 365*"+subTimeList.get(0)+" and "
											+ "datediff(to_date(date), current_date())<= 365*"+subTimeList.get(1))
								   	  .groupBy("riskScenario", "portfolio")
								      .agg(sum(expr("value/1000000")).alias("summe")); 
			
			// Debug Information
			if (debug.equals("debug")) {
		    	System.out.println("fundingLiquidity");
		    	fundingLiquidity.printSchema();
		    	fundingLiquidity.show();}
			
			// Initiale Zeitr�ume Aggregieren
			fundingLiquidity2 = fundingLiquidity2.join(fundingLiquidity.groupBy("portfolio"+i)
																	   .agg(mean("summe").alias("Mittelwert_"+timeList.get(i)),
																	   		max("summe").alias("Maximum_"+timeList.get(i)),
																	   		min("summe").alias("Minimum_"+timeList.get(i))),  
												       "portfolio")
												.orderBy("portfolio");

			// Debug Info
			if (debug.equals("debug")) {
		    	System.out.println("fundingLiquidity");
		    	fundingLiquidity2.printSchema();
		    	fundingLiquidity2.show();
			}

		}
			
		//Output generieren:
	    if(output.equals("parquet")){
	    	fundingLiquidity2.write().mode(SaveMode.Overwrite).parquet(outputPath + "fundingLiquidity.parquet");
	    }
	    else {
	    	fundingLiquidity2.write().mode(SaveMode.Overwrite).csv(outputPath + "fundingLiquidity.csv");
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