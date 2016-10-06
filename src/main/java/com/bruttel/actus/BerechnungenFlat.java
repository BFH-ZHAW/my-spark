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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class BerechnungenFlat {

  public static void main(String[] args) {
    if (args.length != 10) {
      System.err.println("Usage: eventsFile riskfactorsFile debug logPath ram size knoten");
      System.exit(0);
    }
    //Input Parameter:
    String EventsFile = args[0]; //hdfs://160.85.30.40/user/spark/data/output/events.parquet 
    String riskfactorsFile = args[1]; //hdfs://160.85.30.40/user/spark/data/riskfactors_input.csv 
    String debug = args[2];  //write debug to debug or anything else to not debug
    String logPath =args[3]; //hdfs://160.85.30.40/user/spark/data/ Hier wird das Logfile geschrieben
    String ram =args[4]; //12GB wird fürs Log Gebraucht
    String size =args[5]; //3GB wird fürs Log gebraucht
//    String run =args[6]; //1-10 wird im Code generiert
    String knoten =args[6]; //1-8 wird fürs Log gebraucht
    
    //Klassenname wird wieder verwendet:    
    String className = "com.bruttel.actus.BerechnungenFlat";

    //Create Spark Session
    SparkSession sparkSession = SparkSession
    		.builder()
    		.appName(className)
    		.getOrCreate();
   
    // import risk factor data, map to connector and broadcast
    JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsFile).javaRDD(); // contract data
    JavaPairRDD<String, String[]> riskFactorRDD = riskFactor.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));
   
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(riskFactorRDD.first());
    }
    
	// import eventsFile und als Tabelle zur Verfügung stellen:
    Dataset<Row> eventsFile = sparkSession.read().parquet(EventsFile); // contract data
    eventsFile.createOrReplaceTempView("events");
    
//	//Struktur:
//                DataTypes.createStructField("id", DataTypes.StringType, false),
//                DataTypes.createStructField("date", DataTypes.StringType, false),
//                DataTypes.createStructField("type", DataTypes.StringType, false),
//                DataTypes.createStructField("currency", DataTypes.StringType, false),
//                DataTypes.createStructField("value", DataTypes.DoubleType, false),
//                DataTypes.createStructField("nominal", DataTypes.DoubleType, false),
//                DataTypes.createStructField("accrued", DataTypes.DoubleType, false)
//                });
    

    //Debug Info
    if(debug.equals("debug")){
    	System.out.println("cachedEvents Schema und show");
    	eventsFile.printSchema();
    	eventsFile.show();   
    }
    
    //Vorbereitung fürs Logging:
		//Lsite erstellen
	    List<String> logLines = new ArrayList<String>();  
	    //Zeitmesser:
	    long start;
	    long stop;
     	
    //Nominal Value
   	for (int i=1; i < 11 ; i++){  

	    // for time stopping
	    start = System.currentTimeMillis();
	    sparkSession.sql("SELECT sum(nominal) / 1000000 as NominalInMio FROM actusFlat WHERE type = 'AD0'").show();
		
		//Ende der Zeitmessung:
	    stop = System.currentTimeMillis();
	    //LogingZeile erweitern
	    logLines.add(className+","+ram+","+size+","+knoten+","+i+","+(stop-start));
    
   	}
   	
    
    
	// stop spark Session
  sparkSession.stop();
  
// Logging schreiben. 
  try {
		//  Pfad erstellen
	    URI pfad = new URI("file:///home/user/log.csv");
		//File einlesen
	  List<String> lines = Files.readAllLines(Paths.get(pfad));
	  lines.addAll(logLines);
	  Files.write(Paths.get(pfad), lines);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
 
}
}