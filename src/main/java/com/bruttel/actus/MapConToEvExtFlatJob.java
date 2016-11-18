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
import java.util.Arrays;
import java.util.List;


public class MapConToEvExtFlatJob {

  public static void main(String[] args) {
    if (args.length != 10) {
      System.err.println("Usage: path contractFile riskfactorsFile timespecsFile logFile output debug ram run knoten");
      System.exit(0);
    }
    //Input Parameter:
    String path = args[0]; //hdfs://160.85.30.40/user/spark/data/
    String contracts = args[1]; //contracts_10000000.csv 
    String riskfactors = args[2]; //riskfactors_input.csv 
    String timespecsFile = path.concat(args[3]); //timespecs_input.csv 
    String logFile = args[4]; //timespecs_input.csv 
    String output = args[5]; // parquet oder CSV

    String debug = args[6];  //write debug to debug or anything else to not debug
    //String logPath =args[5]; //hdfs://160.85.30.40/user/spark/data/ Hier wird das Logfile geschrieben
    String ram =args[7]; //12GB wird f�rs Log Gebraucht
    //String size =args[6]; //3GB wird f�rs Log gebraucht
    String run =args[8]; //1-10 wird f�rs Log gebraucht
    String knoten =args[9]; //1-8 wird f�rs Log gebraucht
    
    String outputPath = path.concat("output/");
    String contractsFile = path.concat(contracts); //contracts_10000000.csv 
    String riskfactorsFile = path.concat(riskfactors); //riskfactors_input.csv
    
    //typically you want 2-4 partitions for each CPU -> Each Node has 2 Cores
    int partitions = Integer.parseInt(knoten)*3*2;
    
    //Klassenname wird wieder verwendet:    
    String className = "com.bruttel.actus.MapConToEvExtFlatJob";

    //Create Spark Session
    SparkSession sparkSession = SparkSession
    		.builder()
    		.appName(className)
    		.getOrCreate();

    // for time stopping
    long start = System.currentTimeMillis();
    
    //sparkSession.sparkContext().broadcast(value, evidence$11)
     
    // import and broadcast analysis date
    JavaRDD<String> timeSpecs = sparkSession.read().textFile(timespecsFile).javaRDD(); // analysis time specification
    JavaRDD<String> timeVector = timeSpecs.flatMap(line -> Arrays.asList(line.split(";")).iterator());
    ZonedDateTime _t0 = null;
    try{
    	_t0 = DateConverter.of(timeVector.first());  	

    } catch(Exception e) {
      System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
    }
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(_t0);
    }
    
    // import risk factor data, map to connector
    JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsFile).javaRDD(); // risiko data
    JavaPairRDD<String, String[]> riskFactorRDD = riskFactor.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));
    //Check if Broadcast is useful:
    int riskamount = Integer.parseInt(riskfactors.replace("riskfactors_", "").replace(".csv", ""));
    if(riskamount <= 1000 ){
    	riskFactorRDD.cache();}
    else {
    	riskFactorRDD.repartition(partitions);}
       
	// import contracts data, map to connector
    JavaRDD<String> contractFile = sparkSession.read().textFile(contractsFile).javaRDD(); // contract data
    JavaPairRDD<String, String[]> contractFileRDD = contractFile.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[40], temp.split(";")));
    //Check if Broadcast is useful:
    int contractamount = Integer.parseInt(contracts.replace("contracts_", "").replace(".csv", ""));
    if(contractamount <= 1000 ){
    	contractFileRDD.cache();}
    else {
    	contractFileRDD.repartition(partitions);}
    
    JavaPairRDD<String, Tuple2<String[], String[]>> contractsAndRisk = contractFileRDD.join(riskFactorRDD);
	JavaRDD<Row> events = contractsAndRisk.values().flatMap(new ContToEvExtFlatFunc(_t0)).repartition(partitions);
	
    // Create DataFrame Schema
    StructType eventsSchema = DataTypes
            .createStructType(new StructField[] {
            	DataTypes.createStructField("riskScenario", DataTypes.StringType, false),
            	DataTypes.createStructField("portfolio", DataTypes.StringType, false),
            	DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("date", DataTypes.StringType, false),
                DataTypes.createStructField("type", DataTypes.StringType, false),
                DataTypes.createStructField("currency", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false),
                DataTypes.createStructField("nominal", DataTypes.DoubleType, false),
                DataTypes.createStructField("accrued", DataTypes.DoubleType, false), 
                DataTypes.createStructField("discount", DataTypes.DoubleType, false),   //Diskontierung Zins
                });
    
    // Data Frame erstellen
	Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema);

    //Debug Info
    if(debug.equals("debug")){
    	System.out.println("cachedEvents Schema und show");
	    cachedEvents.printSchema();
	    cachedEvents.show();   
    }
    
    // DataFrames can be saved as Parquet files, maintaining the schema information.
    
    if(output.equals("parquet")){
    	cachedEvents.write().parquet(outputPath + "events.parquet");
    }
    else {
    	cachedEvents.write().csv(outputPath + "events.csv");
    }
	
	//Ende der Zeitmessung:
    long stop = System.currentTimeMillis();
 	
	
//  //Hier wird die Zeit f�r das Logging mit Parquet weggeschrieben. 
//	try {
//		//File einlesen
//		Dataset<Row> logCSV = sparkSession.read().parquet(logPath+"log");
//	    if(debug.equals("debug")){
//	    	logCSV.show();
//	    	}
//	    //Zus�tzliche Zeile erstellen
//		Dataset<Row> logNewLine = sparkSession.sql("SELECT '"+className+"','"+ram+"','"+size+"','"+knoten+"','"+run+"','"+(stop-start)+"'");
//	    if(debug.equals("debug")){
//	    	logNewLine.show();
//	    	}
//		logCSV = logCSV.union(logNewLine);
//	    if(debug.equals("debug")){
//	    	logCSV.show();
//	    	}
//		logCSV.write().mode("Append").parquet(logPath+"log");
//	} catch (Exception e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
  
//Data is now ready and it's possible to query the data:
  
  //For SQL Querying:
	//cachedEvents.registerTempTable("events");
//  cachedEvents.createOrReplaceTempView("events");
 
//	results.registerTempTable("events");
//	DataFrame dfCount = 	results
//				.sqlContext().sql("SELECT COUNT(*) "
//								+ "FROM events ");
//	dfCount.show();

	// stop spark Session
  sparkSession.stop();
  
  //Alternativ Logging to Filestore
  //Hier wird die Zeit f�r das Logging weggeschrieben. 
	try {
		//  Pfad erstellen
	    URI pfad = new URI("file:///home/user/".concat(logFile));
		//File einlesen
	  List<String> lines = Files.readAllLines(Paths.get(pfad));
	  lines.add(className+","+output+","+ram+","+contracts+","+riskfactors+","+knoten+","+run+","+(stop-start));
	  Files.write(Paths.get(pfad), lines);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
 
}
}