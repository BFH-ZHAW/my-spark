/**
Erweiterung von: MapContractsJob_V3_0 
Datenverarbeitung: Gefiltertes File -> FLATMAP 
Output: Einzelne Events & Diskontfaktor, Portofolio und Risikoszenarien
Input: Dataset
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.actus.conversion.DateConverter;

import java.util.List;

import javax.time.calendar.ZonedDateTime;

public class MapContractsJob_V4_0 {

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
		String className = "com.bruttel.actus.MapContractsJob_V4_0";

		// Create Spark Session
		SparkSession sparkSession = SparkSession.builder().appName(className).getOrCreate();

		// for time stopping
		long start = System.currentTimeMillis();

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

		// workaround:
		final ZonedDateTime t0 = _t0;

		// import risk factor data as Dataframe and count
		Dataset<Row> riskfactorsFile = sparkSession.read().option("header", true).option("sep", ";")
				.csv(riskfactorsPath);
		long riskfactorsAmount = riskfactorsFile.count() / 4;

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println(
					"riskfactorsFile printSchema() und show() von Total:" + riskfactorsAmount + " Risikoszenarien");
			riskfactorsFile.printSchema();
			riskfactorsFile.show();
		}

		// import contract data as Dataframe and count
		Dataset<Row> contractsFile = sparkSession.read().option("header", true).option("sep", ";").csv(contractsPath);
		long contractsAmount = contractsFile.count();

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println("contractsFile printSchema() und show() von Total:" + contractsAmount + " Contracts");
			contractsFile.printSchema();
			contractsFile.show();
		}

		// Schema
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

		// Abhängig der Grösse der beiden files wird das kleinere mitgegeben. Da
		// Risikofaktoren performanter sind müssen Sie 10 mal grösser sein.
		// Mehr Risikofaktoren als Contracts -> Beim Aufruf werden Contracts
		// mitgegeben.
		if (riskfactorsAmount > contractsAmount * 10) {
			// Eine Liste der Währungen wird für die Sortierung generiert.
			contractsFile.createOrReplaceTempView("contracts");
			List<Row> marketObject = sparkSession.sql("SELECT DISTINCT(MarketObjectCodeRateReset) FROM contracts")
					.collectAsList();

			// Debug Info
			if (debug.equals("debug")) {
				System.out.println("marketobject");
				System.out.println(marketObject.get(0).getString(0));
			}

			// Je Währung wird ein eigener Druchgang (Parallel) gemacht.
			marketObject.parallelStream().forEach(mO -> {
				// Daten Filtern
				Dataset<Row> riskfactorsFiltererd = riskfactorsFile
						.filter(riskfactorsFile.col("MarketObjectCode").equalTo(mO.getString(0)));
				Dataset<Row> contractsFiltererd = contractsFile
						.filter(contractsFile.col("MarketObjectCodeRateReset").equalTo(mO.getString(0)));

				// Flatmap und Dataframe
				JavaRDD<Row> events = riskfactorsFiltererd.javaRDD()
						.flatMap(new MapFunction_V4_contractsfile(contractsFiltererd.collectAsList(), t0));
				Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema);

				// Debug Info
				if (debug.equals("debug")) {
					System.out.println("cachedEvents Schema und show");
					cachedEvents.printSchema();
					cachedEvents.show();
				}

				// Output erstellen.
				if (output.equals("parquet")) {
					cachedEvents.write().mode("append").parquet(outputPath + "events.parquet");
				} else {
					cachedEvents.write().csv(outputPath + "events.csv");
				}

			});

		}
		// Mehr Contracts als Risikofaktoren - > Beim Aufruf werden
		// Risikofaktoren mitgegeben.
		else {
			// Eine Liste der Währungen wird für die Sortierung generiert.
			riskfactorsFile.createOrReplaceTempView("riskfactors");
			List<Row> marketObject = sparkSession.sql("SELECT DISTINCT(MarketObjectCode) FROM riskfactors")
					.collectAsList();

			// Für jede Währung
			marketObject.forEach(mO -> {
				// Daten Filtern
				Dataset<Row> riskfactorsFiltererd = riskfactorsFile
						.filter(riskfactorsFile.col("MarketObjectCode").equalTo(mO.getString(0)));
				Dataset<Row> contractsFiltererd = contractsFile
						.filter(contractsFile.col("MarketObjectCodeRateReset").equalTo(mO.getString(0)));

				// Debug Info
				if (debug.equals("debug")) {
					System.out.println(riskfactorsFiltererd.first().getString(1)); // "riskSet",
				}
				// Flatmap und Dataframe
				JavaRDD<Row> events = contractsFiltererd.javaRDD()
						.flatMap(new MapFunction_V4_riskfile(riskfactorsFiltererd.collectAsList(), t0));
				Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema);

				// Debug Info
				if (debug.equals("debug")) {
					System.out.println("cachedEvents Schema und show");
					cachedEvents.printSchema();
					cachedEvents.show();
				}

				// Output erstellen
				if (output.equals("parquet")) {
					cachedEvents.write().mode("append").parquet(outputPath + "events.parquet");
				} else {
					cachedEvents.write().csv(outputPath + "events.csv");
				}

			});
		}

		// Ende der Zeitmessung:
		long stop = System.currentTimeMillis();

		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		new WriteLog(logFile, className, output, ram, contracts, riskfactors, knoten, run, start, stop);

	}
}