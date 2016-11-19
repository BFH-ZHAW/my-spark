/**
Führt lediglich den Join auf basis der Dataframes aus.
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JoinConDF {

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
		String logFile = args[4]; // logX.csv (Name des Logfiles -> wird fürsLog Gebraucht)
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
		String className = "com.bruttel.actus.JoinConDF";

		// Create Spark Session
		SparkSession sparkSession = SparkSession.builder().appName(className).getOrCreate();

		// for time stopping
		long start = System.currentTimeMillis();

		// import risk factor data as Dataframe and count
		Dataset<Row> riskfactorsFile = sparkSession.read().option("header", true).option("sep", ";")
				.csv(riskfactorsPath);

		// import contract data as Dataframe and count
		Dataset<Row> contractsFile = sparkSession.read().option("header", true).option("sep", ";").csv(contractsPath);

		// Join der Files
		Dataset<Row> filesJoin = contractsFile.join(riskfactorsFile,
				riskfactorsFile.col("MarketObjectCode").equalTo(contractsFile.col("marketObjectCodeRateReset")));

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println("cachedEvents Schema und show");
			filesJoin.printSchema();
			filesJoin.show();
		}
		filesJoin.count();
		System.out.println(filesJoin.count());
		
		// Ende der Zeitmessung:
		long stop = System.currentTimeMillis();

		// DataFrames can be saved as Parquet files, maintaining the schema
		// information.
		if (output.equals("parquet")) {
			filesJoin.write().parquet(outputPath + "joinDF.parquet");
		} else {
			filesJoin.write().csv(outputPath + "joinDF.csv");
		}
		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		new WriteLog(logFile, className, output, ram, contracts, riskfactors, knoten, run, start, stop);

	}
}