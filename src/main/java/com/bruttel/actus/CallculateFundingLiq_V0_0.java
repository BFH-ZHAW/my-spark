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

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class CallculateFundingLiq_V0_0 {

	public static void main(String[] args) {
		if (args.length < 7) {
			System.err.println("Usage: path logFile output debug ram run knoten zeitraum");
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
		String zeitraum = "0-1,1-2,2-5";
		if(args.length==8){
			zeitraum = args[7];  //1-2!2-5!5-6 (Verschiedene Zeitangaben, mit ! getrennt) -> Wird 
		}
		
		// Pfade
		String outputPath = path.concat("output/"); // Kompletter Pfad zum Output Path


		// Klassenname wird wieder verwendet:
		String className = "com.bruttel.actus.CallculateFundingLiq_V0_0";

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
		
		//Zeiträume definieren
		List<String> timeList = Arrays.asList(zeitraum.split(","));
		List<String> subTimeList = Arrays.asList(timeList.get(0).split("-"));
			
		//SQL Query für den initialen Zeitraum
		String sqlquery = "SELECT riskScenario, portfolio, sum(value)/1000000 as FundLiqInMio"
						+ " FROM events "
						+ " WHERE datediff(to_date(date), current_date())>  365*"+subTimeList.get(0)+" and "
						+ "      datediff(to_date(date), current_date())<= 365*"+subTimeList.get(1)
						+ " GROUP BY riskScenario, portfolio";
		//SQL Query ausführen Table erstellen
		Dataset<Row> fundingLiquidity = sparkSession.sql(sqlquery).cache();
		fundingLiquidity.createOrReplaceTempView("fundingLiquidity");
		
		// Debug Information
		if (debug.equals("debug")) {
	    	System.out.println("fundingLiquidity");
	    	fundingLiquidity.printSchema();
	    	fundingLiquidity.show();
	    	}
		
		// SQL Query für den initialen Zeitraum und die Aggregation
		String sqlquery2 = "SELECT portfolio "
						+ "			,mean(FundLiqInMio) as MeanFundingLiquidityInMio_"+0
						+ "			,max(FundLiqInMio) as MaxFundingLiquidityInMio_"+0
						+ "			,min(FundLiqInMio) as MinFundingLiquidityInMio_"+0
						+ " FROM fundingLiquidity "
						+ " GROUP BY  portfolio ";
			
		//SQL Query ausführen Table erstellen
		Dataset<Row> fundingLiquidity2 = sparkSession.sql(sqlquery2).cache();
		fundingLiquidity2.createOrReplaceTempView("fundingLiquidityMean_0");
		
		// Debug Information
		if (debug.equals("debug")) {
	    	System.out.println("fundingLiquidity2");
	    	fundingLiquidity2.printSchema();
	    	fundingLiquidity2.show();
	    	}
		
		//Ieration pro weiteren Zeitraum		
		for(int i=1; i<timeList.size(); i++){
			subTimeList = Arrays.asList(timeList.get(i).split("-"));
			//SQL Query für den initialen Zeitraum
			sqlquery = "SELECT riskScenario, portfolio, sum(value)/1000000 as FundLiqInMio"
					+ " FROM events "
					+ " WHERE datediff(to_date(date), current_date())>  365*"+subTimeList.get(0)+" and "
					+ "      datediff(to_date(date), current_date())<= 365*"+subTimeList.get(1)
					+ " GROUP BY riskScenario, portfolio";
			//SQL Query ausführen Table erstellen
			Dataset<Row> fundingLiquidity3 = sparkSession.sql(sqlquery).cache();
			fundingLiquidity3.createOrReplaceTempView("fundingLiquidity");
			
			// Debug Information
			if (debug.equals("debug")) {
		    	System.out.println("fundingLiquidity3");
		    	fundingLiquidity3.printSchema();
		    	fundingLiquidity3.show();
		    	}
			
			// SQL Query für den initialen Zeitraum und die Aggregation
			sqlquery2 = "SELECT portfolio "
					+ "			,mean(FundLiqInMio) as MeanFundingLiquidityInMio_"+i
					+ "			,max(FundLiqInMio) as MaxFundingLiquidityInMio_"+i
					+ "			,min(FundLiqInMio) as MinFundingLiquidityInMio_"+i
					+ " FROM fundingLiquidity "
					+ " GROUP BY  portfolio ";
			
			//SQL Query ausführen Table erstellen
			Dataset<Row> fundingLiquidity4 = sparkSession.sql(sqlquery2).cache();
			fundingLiquidity4.createOrReplaceTempView("fundingLiquidityMean_"+i);
			
			// Debug Information
			if (debug.equals("debug")) {
		    	System.out.println("fundingLiquidity4");
		    	fundingLiquidity4.printSchema();
		    	fundingLiquidity4.show();
		    	}
			
			//SQL für den Join 
			String sqlquery5 = "SELECT fM0.*  "
							+ "		   ,MeanFundingLiquidityInMio_"+i
							+ "		   ,MaxFundingLiquidityInMio_"+i
							+ "		   ,MinFundingLiquidityInMio_"+i
							 + " FROM fundingLiquidityMean_0 AS fM0"
							 + "	 JOIN fundingLiquidityMean_"+i+" AS fM"+i
							 + "	 ON fM0.portfolio = fM"+i+".portfolio "
							 + " ORDER BY fM0.portfolio";
			
			//SQL Query ausführen Table erstellen
			fundingLiquidity2 = sparkSession.sql(sqlquery5).cache();
			fundingLiquidity2.createOrReplaceTempView("fundingLiquidityMean_0");
		
			// Debug Info
			if (debug.equals("debug")) {
		    	System.out.println("JoinedFundingLiquidity");
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