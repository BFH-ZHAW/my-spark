/**
Initiale Version
Datenverarbeitung: MAP
Output: Arrays
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.actus.conversion.DateConverter;

import javax.time.calendar.ZonedDateTime;
import scala.Tuple2;

import java.util.Arrays;

public class MapContractsJob_V0_0 {

	public static void main(String[] args) {
		if (args.length != 10) {
			System.err.println("Usage: path contractFile riskfactorsFile timespecsFile logFile output debug ram run knoten");
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
		String className = "com.bruttel.actus.MapContractsJob_V0_0";

		// Create Spark Session
		SparkSession sparkSession = SparkSession.builder().appName(className).getOrCreate();

		// for time stopping
		long start = System.currentTimeMillis();

		// import and broadcast analysis date
		JavaRDD<String> timeSpecs = sparkSession.read().textFile(timespecsPath).javaRDD(); // analysis timespecification
		JavaRDD<String> timeVector = timeSpecs.flatMap(line -> Arrays.asList(line.split(";")).iterator());
		ZonedDateTime _t0 = null;
		try {
			_t0 = DateConverter.of(timeVector.first());
			// _t0 = DateConverter.of(timeVector[0]);
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
		}
		// Debug Info
		if (debug.equals("debug")) {
			System.out.println(_t0);
		}

		// import risk factor data, map to connector
		JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsPath).javaRDD(); // contractdata
		JavaPairRDD<String, String[]> riskFactorRDD = riskFactor
				.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println(riskFactorRDD.first());
		}

		// import and map contract data to contract event results
		JavaRDD<String> contractFile = sparkSession.read().textFile(contractsPath).javaRDD(); // contractdata
		JavaRDD<Row> events = contractFile.map(new MapFunction_V0(_t0, riskFactorRDD.collectAsMap()));

		// convert to DataFrame
		StructType eventsSchema = DataTypes.createStructType(new StructField[] {
				DataTypes.createStructField("id", DataTypes.createArrayType(DataTypes.StringType), false),
				DataTypes.createStructField("date", DataTypes.createArrayType(DataTypes.StringType), false),
				DataTypes.createStructField("type", DataTypes.createArrayType(DataTypes.StringType), false),
				DataTypes.createStructField("currency", DataTypes.createArrayType(DataTypes.StringType), false),
				DataTypes.createStructField("value", DataTypes.createArrayType(DataTypes.DoubleType), false),
				DataTypes.createStructField("nominal", DataTypes.createArrayType(DataTypes.DoubleType), false),
				DataTypes.createStructField("accrued", DataTypes.createArrayType(DataTypes.DoubleType), false) });

		// Data Frame erstellen
		Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema).cache();

		// Debug Info
		if (debug.equals("debug")) {
			cachedEvents.printSchema();
			cachedEvents.show();
		}

		//Output generieren:
	    if(output.equals("parquet")){
	    	cachedEvents.write().parquet(outputPath + "events.parquet");
	    }
	    else {
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