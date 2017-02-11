/**
Erweiterung von: MapContractsJob_V1_0 
Datenverarbeitung: JOIN -> FLATMAP 
Performance: Normal
Output: Einzelne Events & Diskontfaktor, Portofolio und Risikoszenarien
Input: RDD's
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.actus.conversion.DateConverter;

import javax.time.calendar.ZonedDateTime;

public class MapContractsPerformance_V2_2 {

	public static void main(String[] args) {
		if (args.length != 10) {
			System.err.println(
					"Usage: path contractFile riskfactorsFile timespecsFile logFile output debug ram run knoten");
			System.exit(0);
		}
		// Input Parameter:
		String path = args[0]; // hdfs://160.85.30.40/user/spark/data/
		String contracts = args[1]; // contracts_100000.csv
		String riskfactors = args[2]; // riskfactors_200.csv
		String timespecs = args[3]; // timespecs_input.csv
		String logFile = args[4]; // logX.csv (Name des Logfiles -> wird fürs Log Gebraucht)
		String output = args[5]; // parquet oder CSV
		String debug = args[6]; // write debug to debug or anything else to not debug
		String ram = args[7]; // 12GB (Ram Pro Executor -> wird fürs LogGebraucht)
		String run = args[8]; // 1-10 (Durchgang -> wird fürs Log gebraucht)
		String knoten = args[9]; // 1-8 (Anzahl aktive Knoten -> wird fürs Log gebraucht)

		// Pfade
		String timespecsPath = path.concat(timespecs);
		String outputPath = path.concat("output/"); // Kompletter Pfad zum Output Path
		String contractsPath = path.concat(contracts); // Kompletter Pfad zum Contractsfile
		String riskfactorsPath = path.concat(riskfactors); // Kompletter Pfad zum Riskfactor File

		// Klassenname wird wieder verwendet:
		String className = "com.bruttel.actus.MapContractsPerformance_V2_2";

		// Create Spark Session
		SparkSession sparkSession = SparkSession.builder().appName(className).getOrCreate();

		// for time stopping
		long start = System.currentTimeMillis();

		// import and broadcast analysis date
		Dataset<Row> timespecsFile = sparkSession.read().option("header", false).csv(timespecsPath);
	    ZonedDateTime _t0 = null;
	    try{_t0 = DateConverter.of(timespecsFile.first().getString(0));  	
	    } catch(Exception e) {
	      System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
	    }
		// Debug Info
		if (debug.equals("debug")) {
			System.out.println(_t0);
		}

		// import risk factor data, map to connector
		Dataset<Row> riskfactorsFile = sparkSession.read().option("header", true).option("sep",";").csv(riskfactorsPath);    
	   	long riskfactorsAmount = riskfactorsFile.count()/4;
	   	
	   	//Debug Information
	    if(debug.equals("debug")){
	    	System.out.println("riskfactorsFile printSchema() und show() von Total:"+riskfactorsAmount+" Risikoszenarien");
	    	riskfactorsFile.printSchema();
	    	riskfactorsFile.show();  
	    }

		// IMPORT TEIL
			//Perfomrance Messung
			long startRead = System.currentTimeMillis();
			Dataset<Row> contractsFile = sparkSession.read().option("header", true).option("sep",";").csv(contractsPath);
		   	long contractsAmount = contractsFile.count();
			// Log Schreiben
			new WriteLog(logFile, className.concat("_Read"), output, ram, String.valueOf(contractsFile.count()), riskfactors, knoten, "1", startRead, System.currentTimeMillis());
		
		//Debug Information
	    if(debug.equals("debug")){
	    	System.out.println("contractsFile printSchema() und show() von Total:"+contractsAmount+" Contracts");
	    	contractsFile.printSchema();
	    	contractsFile.show();  
	    } 
		
		// JOIN TEIL
			//Perfomrance Messung
			long startJoin = System.currentTimeMillis();
		    Dataset<Row> filesJoin = contractsFile.join(riskfactorsFile, riskfactorsFile.col("MarketObjectCode").equalTo(contractsFile.col("marketObjectCodeRateReset")));
		// Log Schreiben
			new WriteLog(logFile, className.concat("_Join"), output, ram, String.valueOf(filesJoin.count()), riskfactors, knoten, "1", startJoin, System.currentTimeMillis());
				
		//FLATMAP TEIL
			//Perfomrance Messung
			long startFlatmap = System.currentTimeMillis();
			JavaRDD<Row> events = filesJoin.javaRDD().flatMap(new MapFunction_V3(_t0));
			// Log Schreiben
					new WriteLog(logFile, className.concat("_Flatmap"), output, ram, String.valueOf(events.count()), riskfactors, knoten, "1", startFlatmap, System.currentTimeMillis());
						
		// Create DataFrame Schema
		StructType eventsSchema = DataTypes.createStructType(
				new StructField[] { DataTypes.createStructField("riskScenario", DataTypes.StringType, false),
						DataTypes.createStructField("portfolio", DataTypes.StringType, false),
						DataTypes.createStructField("id", DataTypes.StringType, false),
						DataTypes.createStructField("date", DataTypes.StringType, false),
						DataTypes.createStructField("type", DataTypes.StringType, false),
						DataTypes.createStructField("currency", DataTypes.StringType, false),
						DataTypes.createStructField("value", DataTypes.DoubleType, false),
						DataTypes.createStructField("nominal", DataTypes.DoubleType, false),
						DataTypes.createStructField("accrued", DataTypes.DoubleType, false),
						DataTypes.createStructField("discount", DataTypes.DoubleType, false), 
				});

		// Data Frame erstellen
			//Perfomrance Messung
			long startDataFrame = System.currentTimeMillis();
		Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema);
		// Log Schreiben
		new WriteLog(logFile, className.concat("_Datframe"), output, ram, String.valueOf(cachedEvents.count()), riskfactors, knoten, "1", startDataFrame, System.currentTimeMillis());
				
		// Debug Info
		if (debug.equals("debug")) {
			System.out.println("cachedEvents Schema und show");
			cachedEvents.printSchema();
			cachedEvents.show();
		}

		// DataFrames can be saved as Parquet files, maintaining the schema information.
		if (output.equals("parquet")) {
				//Perfomrance Messung
				long startParqut = System.currentTimeMillis();
			cachedEvents.write().mode(SaveMode.Overwrite).parquet(outputPath + "events.parquet");
			// Log Schreiben
			new WriteLog(logFile, className.concat("_WriteParquet"), output, ram, String.valueOf(cachedEvents.count()), riskfactors, knoten, "1", startParqut, System.currentTimeMillis());
				
		} else {
				//Perfomrance Messung
				long startCSV = System.currentTimeMillis();
			cachedEvents.write().mode(SaveMode.Overwrite).csv(outputPath + "events.csv");
			// Log Schreiben
						new WriteLog(logFile, className.concat("_WriteCSV"), output, ram, String.valueOf(cachedEvents.count()), riskfactors, knoten, "1", startCSV, System.currentTimeMillis());
		}

		// Ende der Zeitmessung:
		long stop = System.currentTimeMillis();

		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		new WriteLog(logFile, className, output, ram, contracts, riskfactors, knoten, run, start, stop);

	}
}