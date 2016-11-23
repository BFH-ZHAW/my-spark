/**
Führt lediglich den Join auf basis der Dataframes aus.
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.time.calendar.ZonedDateTime;

import org.actus.conversion.DateConverter;
import org.apache.spark.api.java.JavaRDD;
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
		String logFile = args[4]; // logX.csv (Name des Logfiles -> wird fürsLog
									// Gebraucht)
		String output = args[5]; // parquet oder CSV
		String debug = args[6]; // write debug to debug or anything else to not
								// debug
		String ram = args[7]; // 12GB (Ram Pro Executor -> wird fürs
								// LogGebraucht)
		String run = args[8]; // 1-10 (Durchgang -> wird fürs Log gebraucht)
		String knoten = args[9]; // 1-8 (Anzahl aktive Knoten -> wird fürs Log
									// gebraucht)

		// Pfade
		String timespecsPath = path.concat(timespecs);
		String outputPath = path.concat("output/"); // Kompletter Pfad zum
													// Output Path
		String contractsPath = path.concat(contracts); // Kompletter Pfad zum
														// Contractsfile
		String riskfactorsPath = path.concat(riskfactors); // Kompletter Pfad
															// zum Riskfactor
															// File

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

		// Log Schreiben
		new WriteLog(logFile, className, output, ram, contracts, riskfactors, knoten, run, start, stop);
		
//Ende Teil 1

		// Klassenname wird wieder verwendet:
		String className2 = "com.bruttel.actus.JoinConDF_MAP";


		// sparkSession.sparkContext().broadcast(value, evidence$11)

		// import and broadcast analysis date als Dataframe
		Dataset<Row> timespecsFile = sparkSession.read().option("header", false).csv(timespecsPath);
		ZonedDateTime _t0 = null;
		try {
			_t0 = DateConverter.of(timespecsFile.first().getString(0));
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
		}
		// Debug Info
		if (debug.equals("debug")) {
			System.out.println(_t0);
		}

		// Lesen des zweiten Files
		Dataset<Row> filesRead = sparkSession.read().parquet(outputPath.concat("joinDF.parquet"));

		
		// Debug Information
		if (debug.equals("debug")) {
			System.out.println("filesRead printSchema() und show() von Total:" + filesRead.count() + " Contracts");
			filesRead.printSchema();
			filesRead.show();
		}

		// for time stopping without respecting filereadingtime
		long start2 = System.currentTimeMillis();

		// Flatmap
		JavaRDD<Row> events = filesRead.javaRDD().flatMap(new MapFunction_V3(_t0));

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
		long stop2 = System.currentTimeMillis();

		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		new WriteLog(logFile, className2, output, ram, contracts, riskfactors, knoten, run, start2, stop2);

	}
}