/**
Neue Version
Datenverarbeitung: FLATMAP
Output: Einzelne Events
Input: RDD's
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.actus.conversion.DateConverter;

import javax.time.calendar.ZonedDateTime;
import scala.Tuple2;

import java.util.Arrays;

public class MapContractsJob_V1_0 {

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
		String className = "com.bruttel.actus.MapContractsJob_V1_0";

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

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println(riskFactorRDD.first());
		}

		// import and FLATMAP contract data to contract event results
		JavaRDD<String> contractFile = sparkSession.read().textFile(contractsPath).javaRDD();  
		JavaRDD<Row> events = contractFile.flatMap(new MapFunction_V1(_t0, riskFactorRDD.collectAsMap()));

		// Create DataFrame Schema
		StructType eventsSchema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("id", DataTypes.StringType, false),
						DataTypes.createStructField("date", DataTypes.StringType, false),
						DataTypes.createStructField("type", DataTypes.StringType, false),
						DataTypes.createStructField("currency", DataTypes.StringType, false),
						DataTypes.createStructField("value", DataTypes.DoubleType, false),
						DataTypes.createStructField("nominal", DataTypes.DoubleType, false),
						DataTypes.createStructField("accrued", DataTypes.DoubleType, false) });

		// Data Frame erstellen
		Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema).cache();

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println("cachedEvents Schema und show");
			cachedEvents.printSchema();
			cachedEvents.show();
		}

		// DataFrames can be saved as Parquet files, maintaining the schema information.
		if (output.equals("parquet")) {
			cachedEvents.write().mode(SaveMode.Overwrite).parquet(outputPath + "events.parquet");
		} else {
			cachedEvents.write().mode(SaveMode.Overwrite).csv(outputPath + "events.csv");
		}

		// Ende der Zeitmessung:
		long stop = System.currentTimeMillis();

		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		new WriteLog(logFile, className, output, ram, contracts, riskfactors, knoten, run, start, stop);

	}
}