/**
 * Berechnet den Zins aller Eents und Gruppiert auf Basis der Portfolios
 * Datenverarbeitung: SQL
 * Output: Bildschirm
 * Input: CSV
 * @author Daniel Bruttel
 * @version 1.0
 */
package com.bruttel.actus;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class CallculateDataFrameCSV {

	public static void main(String[] args) throws Exception {
		if (args.length < 8) {
			throw new Exception("Usage: Filename activity path logFile output debug ram run knoten zeitraum");
		}
		String csvFile = args[0]; // Filename
		String activity = args[1]; // Group or Count aktion auf dem REsultat
		String path = args[2]; // hdfs://160.85.30.40/user/spark/data/
		String logFile = args[3]; // logX.csv (Name des Logfiles -> wird fürs Log Gebraucht)
		String output = args[4]; // parquet oder CSV
		String debug = args[5]; // write debug to debug or anything else to not debug
		String ram = args[6]; // 12GB (Ram Pro Executor -> wird fürs LogGebraucht)
		String run = args[7]; // 1-10 (Durchgang -> wird fürs Log gebraucht)
		String knoten = args[8]; // 1-8 (Anzahl aktive Knoten -> wird fürs Log gebraucht)
		String loops = "1";
		if (args.length == 9) {
			loops = args[7]; // Anzahl der Loops um die aktion auszuführen
		}

		// Pfade
		String outputPath = path.concat("output/"); // Kompletter Pfad zum
													// Output Path
		String filePath = path.concat(csvFile);

		// Klassenname wird wieder verwendet:
		String className = "com.bruttel.actus.CallculateDataFrameCSV";

		// Create Spark Session
		SparkSession sparkSession = SparkSession.builder().appName(className).getOrCreate();

		// for time stopping
		long start = System.currentTimeMillis();

		// DataFrame aus dem CSV File erstellen
		Dataset<Row> dsPortfolio = sparkSession.read().option("header", true).option("sep", ",").csv(filePath); 
		// Temporäre Tabelle für das SQL erstellen
		dsPortfolio.createOrReplaceTempView("portfolio");

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println("events geladen");
			dsPortfolio.printSchema();
			dsPortfolio.show();
		}

		// SQL abhängig von Activity:
		String sqlquery;
		if (activity.equals("count")) {
			sqlquery = "SELECT count(*) " 
					 + "FROM portfolio ";
		} else {
			sqlquery = "SELECT Portfolio, " 
					+ "SUM(R.Nennwert * POW (R.Zins, R.Laufzeit)) as Istwert, "
					+ "COUNT(Distinct Nr) as Anzahl, " 
					+ "FROM portfolio "	
					+ "GROUP BY Portfolio ";
		}

		// Query ausführen
		Dataset<Row> dsResult = dsPortfolio.sqlContext().sql(sqlquery).cache();

		// Hier werden die Loops generiert.:
		int loop = Integer.parseInt(loops);
		if (loop > 1) {
			for (int i = 1; i < loop; i++) {
				// Start der Zeitmessung
				long startTime = System.currentTimeMillis();

				// SQL Query ausführen
				dsResult = dsPortfolio.sqlContext().sql(sqlquery).cache();
				dsResult.count();

				// Ende der Zeitmessung:
				long endTime = System.currentTimeMillis();
				// Log Ausgabe
				System.out.println("Dauer Durchgang " + i + ": " + (endTime - startTime) + " ms");
				new WriteLog(logFile, className, output, ram, csvFile, "", knoten, String.valueOf(i), startTime,
						endTime);
			}
		}

		// Debug Info
		if (debug.equals("debug")) {
			System.out.println("events geladen");
			dsPortfolio.printSchema();
			dsPortfolio.show();
		}

		// Output generieren:
		if (output.equals("parquet")) {
			dsResult.write().mode(SaveMode.Overwrite).parquet(outputPath + "kapitalertrag.parquet");
		} else {
			dsResult.write().mode(SaveMode.Overwrite).csv(outputPath + "kapitalertrag.csv");
		}

		// Ende der Zeitmessung:
		long stop = System.currentTimeMillis();

		// stop spark Session
		sparkSession.stop();

		// Log Schreiben
		// new WriteLog(logFile, className, output, ram, contracts, riskfactors,
		// knoten, run, start, stop);
		new WriteLog(logFile, className, output, ram, csvFile, "", knoten, run, start, stop);

	}
}