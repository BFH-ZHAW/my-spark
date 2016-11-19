/**
Führt lediglich den Join auf basis der RDDS aus.
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;


public class JoinConRDD {

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
		String className = "com.bruttel.actus.JoinConRDD";

		// Create Spark Session
		SparkSession sparkSession = SparkSession.builder().appName(className).getOrCreate();

		// for time stopping
		long start = System.currentTimeMillis();

		// import risk factor data, map to connector
		JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsPath).javaRDD(); // risiko
																								// data
		JavaPairRDD<String, String[]> riskFactorRDD = riskFactor
				.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));

		// import contracts data, map to connector
		JavaRDD<String> contractFile = sparkSession.read().textFile(contractsPath).javaRDD(); // contract
																								// data
		JavaPairRDD<String, String[]> contractFileRDD = contractFile
				.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[40], temp.split(";")));

		// Actual Join
		JavaPairRDD<String, Tuple2<String[], String[]>> contractsAndRisk = contractFileRDD.join(riskFactorRDD);

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println("Joined Contracts: " + contractsAndRisk.count());
		}
		contractsAndRisk.count();
		System.out.println(contractsAndRisk.count());
		
		// Ende der Zeitmessung:
		long stop = System.currentTimeMillis();

		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		new WriteLog(logFile, className, output, ram, contracts, riskfactors, knoten, run, start, stop);

	}
}