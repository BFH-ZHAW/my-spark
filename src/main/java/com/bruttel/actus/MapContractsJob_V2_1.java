/**
Daten werden mit FlatMap verarbeitet
Erweiterung des Output mit Diskontfaktor, Portfolio und Risikoszenario 
Umsetzung mit einem Join
Partitionierung falls Files zu Gross
Ansonsten Identisch mit MapContractsJob_V1_0 
Output sind Arrays
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.actus.conversion.DateConverter;

import javax.time.calendar.ZonedDateTime;
import scala.Tuple2;

import java.util.Arrays;

public class MapContractsJob_V2_1 {

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
		String className = "com.bruttel.actus.MapContractsJob_V2_1";
		
	    //typically you want 2-4 partitions for each CPU -> Each Node has 2 Cores
	    int partitions = Integer.parseInt(knoten)*3*2;

		// Create Spark Session
		SparkSession sparkSession = SparkSession.builder().appName(className).getOrCreate();

		// for time stopping
		long start = System.currentTimeMillis();

		// import and broadcast analysis date
		JavaRDD<String> timeSpecs = sparkSession.read().textFile(timespecsPath).javaRDD();  
		JavaRDD<String> timeVector = timeSpecs.flatMap(line -> Arrays.asList(line.split(";")).iterator());
		ZonedDateTime _t0 = null;
		try {
			_t0 = DateConverter.of(timeVector.first());

		} catch (Exception e) {
			System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
		}
		// Debug Info
		if (debug.equals("debug")) {
			System.out.println(_t0);
		}

		// import risk factor data, map to connector
		JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsPath).javaRDD();  
		JavaPairRDD<String, String[]> riskFactorRDD = riskFactor
				.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));
		// Check if Broadcast is useful without counting rdd:
		int riskamount = Integer.parseInt(riskfactors.replace("riskfactors_", "").replace(".csv", ""));
		if (riskamount <= 1000) {
			riskFactorRDD.cache();
		} else {
			riskFactorRDD.repartition(partitions);
		}

		// import contracts data, map to connector
		JavaRDD<String> contractFile = sparkSession.read().textFile(contractsPath).javaRDD(); // contract
																								// data
		JavaPairRDD<String, String[]> contractFileRDD = contractFile
				.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[40], temp.split(";")));
		// Check if Broadcast is useful without counting rdd:
		int contractamount = Integer.parseInt(contracts.replace("contracts_", "").replace(".csv", ""));
		if (contractamount <= 1000) {
			contractFileRDD.cache();
		} else {
			contractFileRDD.repartition(partitions);
		}

		JavaPairRDD<String, Tuple2<String[], String[]>> contractsAndRisk = contractFileRDD.join(riskFactorRDD);
		JavaRDD<Row> events = contractsAndRisk.values().flatMap(new MapFunction_V2(_t0)).repartition(partitions);

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
						DataTypes.createStructField("discount", DataTypes.DoubleType, false), // Diskontierung
																								// Zins
				});

		// Data Frame erstellen
		Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema);

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println("cachedEvents Schema und show");
			cachedEvents.printSchema();
			cachedEvents.show();
		}

		// DataFrames can be saved as Parquet files, maintaining the schema
		// information.
		if (output.equals("parquet")) {
			cachedEvents.write().parquet(outputPath + "events.parquet");
		} else {
			cachedEvents.write().csv(outputPath + "events.csv");
		}

		// Ende der Zeitmessung:
		long stop = System.currentTimeMillis();

		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		new WriteLog(logFile, className, output, ram, contracts, riskfactors, knoten, run, start, stop);

	}
}