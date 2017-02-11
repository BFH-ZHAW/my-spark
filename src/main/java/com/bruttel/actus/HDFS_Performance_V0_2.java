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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class HDFS_Performance_V0_2 {

	public static void main(String[] args) {
		if (args.length != 8) {
			System.err.println(
					"Usage: path contractFile logFile output debug ram run knoten");
			System.exit(0);
		}
		// Input Parameter:
		String path = args[0]; // hdfs://160.85.30.40/user/spark/data/
		String contracts = args[1]; // contracts_100000.csv
		String logFile = args[2]; // logX.csv (Name des Logfiles -> wird fürs Log Gebraucht)
		String output = args[3]; // parquet oder CSV
		String debug = args[4]; // write debug to debug or anything else to not debug
		String ram = args[5]; // 12GB (Ram Pro Executor -> wird fürs LogGebraucht)
		String run = args[6]; // 1-10 (Durchgang -> wird fürs Log gebraucht)
		String knoten = args[7]; // 1-8 (Anzahl aktive Knoten -> wird fürs Log gebraucht)

		// Pfade
		String outputPath = path.concat("output/"); // Kompletter Pfad zum Output Path
		String contractsPath = path.concat(contracts); // Kompletter Pfad zum Contractsfile

		// Klassenname wird wieder verwendet:
		String className = "com.bruttel.actus.HDFS_Performance_V0_2";

		// Create Spark Session
		SparkSession sparkSession = SparkSession.builder().appName(className).getOrCreate();

		// for time stopping
		long start = System.currentTimeMillis();

		// IMPORT TEIL
		// Perfomrance Messung
		long startRead = System.currentTimeMillis();
		Dataset<Row> contractsFile = sparkSession.read().option("header", true).option("sep", ";").csv(contractsPath);
		long contractsAmount = contractsFile.count();
		// Log Schreiben
		new WriteLog(logFile, className.concat("_Read"), output, ram, String.valueOf(contractsAmount), "1",
				knoten, run, startRead, System.currentTimeMillis());
		for (int i = 1; i<=100; i++){
			startRead = System.currentTimeMillis();
			contractsFile = contractsFile.union(sparkSession.read().option("header", true).option("sep", ";").csv(contractsPath));
			contractsAmount = contractsFile.count();
			// Log Schreiben
			new WriteLog(logFile, className.concat("_Read"), output, ram, String.valueOf(contractsAmount), "1",
					knoten, run, startRead, System.currentTimeMillis());
		}
		
		

		// Debug Information
		if (debug.equals("debug")) {
			System.out.println("contractsFile printSchema() und show() von Total:" + contractsAmount + " Contracts");
			contractsFile.printSchema();
			contractsFile.show();
		}

		// DataFrames can be saved as Parquet files, maintaining the schema
		// information.
		if (output.equals("parquet")) {
			// Perfomrance Messung
			long startParqut = System.currentTimeMillis();
			contractsFile.write().mode(SaveMode.Overwrite).parquet(outputPath + "contracts.parquet");
			// Log Schreiben
			new WriteLog(logFile, className.concat("_WriteParquet"), output, ram, String.valueOf(contractsAmount),
					"1", knoten, run, startParqut, System.currentTimeMillis());

		} else {
			// Perfomrance Messung
			long startCSV = System.currentTimeMillis();
			contractsFile.write().mode(SaveMode.Overwrite).csv(outputPath + "contracts.csv");
			// Log Schreiben
			new WriteLog(logFile, className.concat("_WriteCSV"), output, ram, String.valueOf(contractsAmount),
					"1", knoten, run, startCSV, System.currentTimeMillis());
		}

		// Ende der Zeitmessung:
		long stop = System.currentTimeMillis();

		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		new WriteLog(logFile, className, output, ram, String.valueOf(contractsAmount), "1", knoten, run, start,
				stop);

	}
}