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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Arrays;
import java.util.List;


public class CallculateFundingLiq_V0_2 {

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
		String className = "com.bruttel.actus.CallculateFundingLiq_V0_2";

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
			
		//SQL Queries initialisieren
		String[] sqlquerys = new String[timeList.size()];
			
		//Ierationen pro Zeitraum		
		for(int i=0; i<timeList.size(); i++){
			List<String> subTimeList = Arrays.asList(timeList.get(i).split("-"));
			
			sqlquerys[i] = "SELECT portfolio "
							+ "			,mean(FundLiqInMio) as MeanFundingLiquidityInMio_"+i
							+ "			,max(FundLiqInMio) as MaxFundingLiquidityInMio_"+i
							+ "			,min(FundLiqInMio) as MinFundingLiquidityInMio_"+i
							+ " FROM ( SELECT riskScenario, portfolio, sum(value)/1000000 as FundLiqInMio"
							+ " 	   FROM events "
							+ " 	   WHERE datediff(to_date(date), current_date())>  365*"+subTimeList.get(0)+" and "
							+ "        datediff(to_date(date), current_date())<= 365*"+subTimeList.get(1)
							+ "        GROUP BY riskScenario, portfolio) AS table"
							+ " GROUP BY  portfolio ";
		}
		
		String select = "SELECT t0.* ";
		String from   = " FROM ("+sqlquerys[0]+") AS t0 "; 
		
		for(int i=1; i<timeList.size(); i++){
			select = select.concat(", MeanFundingLiquidityInMio_"+i
								+ ", MaxFundingLiquidityInMio_"+i
								+ ", MinFundingLiquidityInMio_"+i);
			from = from.concat(" JOIN ("+sqlquerys[i]+") AS t"+i
							  +" ON t0.portfolio = t"+i+".portfolio ");
		}
		
		String sqlquery = select.concat(from.concat(" ORDER BY portfolio"));
		
		// Debug Information
		if (debug.equals("debug")) {
	    	System.out.println("SQL Query:");
	    	System.out.println(sqlquery);
	    	}
		
		//SQL Query ausf�hren Table erstellen
		Dataset<Row> fundingLiquidity = sparkSession.sql(sqlquery).cache();
		fundingLiquidity.createOrReplaceTempView("fundingLiquidity");
			
		// Debug Information
		if (debug.equals("debug")) {
	    	System.out.println("fundingLiquidity_");
	    	fundingLiquidity.printSchema();
	    	fundingLiquidity.show();
	    	}
		
		//Output generieren:
	    if(output.equals("parquet")){
	    	fundingLiquidity.write().mode(SaveMode.Overwrite).parquet(outputPath + "fundingLiquidity.parquet");
	    }
	    else {
	    	fundingLiquidity.write().mode(SaveMode.Overwrite).csv(outputPath + "fundingLiquidity.csv");
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

