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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class MapContractsToEventsJob {

  public static void main(String[] args) {
	    if (args.length != 10) {
	        System.err.println("Usage: contractFile riskfactorsFile timespecsFile outputPath debug logPath ram size run knoten");
	        System.exit(0);
	      }
	      //Input Parameter:
	      String contractsFile = args[0]; //hdfs://160.85.30.40/user/spark/data/contracts_10000000.csv 
	      String riskfactorsFile = args[1]; //hdfs://160.85.30.40/user/spark/data/riskfactors_input.csv 
	      String timespecsFile = args[2]; //hdfs://160.85.30.40/user/spark/data/timespecs_input.csv 
	      String outputPath = args[3]; //hdfs://160.85.30.40/user/spark/data/output/; 
	      String debug = args[4];  //write debug to debug or anything else to not debug
	      String logPath =args[5]; //hdfs://160.85.30.40/user/spark/data/ Hier wird das Logfile geschrieben
	      String ram =args[6]; //12GB wird fürs Log Gebraucht
	      String size =args[7]; //3GB wird fürs Log gebraucht
	      String run =args[8]; //1-10 wird fürs Log gebraucht
	      String knoten =args[9]; //1-8 wird fürs Log gebraucht
	    
	    //Klassenname wird wieder verwendet:    
	    String className = "com.bruttel.actus.MapContractsToEventsJob"; 
	    
    //Create Spark Session
    SparkSession sparkSession = SparkSession
    		.builder()
    		.appName(className)
    		.getOrCreate();
    
    // for time stopping
    long start = System.currentTimeMillis();
     
    // import and broadcast analysis date
    JavaRDD<String> timeSpecs = sparkSession.read().textFile(timespecsFile).javaRDD(); // analysis time specification
    JavaRDD<String> timeVector = timeSpecs.flatMap(line -> Arrays.asList(line.split(";")).iterator());
    ZonedDateTime _t0 = null;
    try{
    	_t0 = DateConverter.of(timeVector.first());  	
//      _t0 = DateConverter.of(timeVector[0]);
    } catch(Exception e) {
      System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
    }
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(_t0);
    }
    
    // import risk factor data, map to connector
    JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsFile).javaRDD(); // contract data
    JavaPairRDD<String, String[]> riskFactorRDD = riskFactor.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));
  
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(riskFactorRDD.first());
    }
    
	// import and map contract data to contract event results
    JavaRDD<String> contractFile = sparkSession.read().textFile(contractsFile).javaRDD(); // contract data
    JavaRDD<Row> events = contractFile.map(new ContractToEventsFunction(_t0, riskFactorRDD.collectAsMap()));
	
    // convert to DataFrame
    StructType eventsSchema = DataTypes
            .createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("date", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("type", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("currency", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("value", DataTypes.createArrayType(DataTypes.DoubleType), false),
                DataTypes.createStructField("nominal", DataTypes.createArrayType(DataTypes.DoubleType), false),
                DataTypes.createStructField("accrued", DataTypes.createArrayType(DataTypes.DoubleType), false)});
    
    //Data Frame erstellen
    Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema).cache();
 	
    //Debug Info
    if(debug.equals("debug")){
	    cachedEvents.printSchema();
	    cachedEvents.show();   
    }
    
 // DataFrames can be saved as Parquet files, maintaining the schema information.
 	cachedEvents.write().parquet(outputPath + "events.parquet");
 	
	
	//Ende der Zeitmessung:
    long stop = System.currentTimeMillis();
 	
//    //Hier wird die Zeit für das Logging mit Parquet weggeschrieben. 
//	try {
//		//File einlesen
//		Dataset<Row> logCSV = sparkSession.read().parquet(logPath+"log");
//	    if(debug.equals("debug")){
//	    	logCSV.show();
//	    	}
//	    //Zusätzliche Zeile erstellen
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
//    cachedEvents.createOrReplaceTempView("events");
   
// 	results.registerTempTable("events");
// 	DataFrame dfCount = 	results
//				.sqlContext().sql("SELECT COUNT(*) "
//								+ "FROM events ");
// 	dfCount.show();

	// stop spark Session
    sparkSession.stop();
    
    //Alternativ Logging to Filestore
    //Hier wird die Zeit für das Logging weggeschrieben. 
  	try {
  		//  Pfad erstellen
  	    URI pfad = new URI("file:///home/user/log.csv");
  		//File einlesen
  	  List<String> lines = Files.readAllLines(Paths.get(pfad));
	  lines.add(className+","+ram+","+size+","+knoten+","+run+","+(stop-start));
  	  Files.write(Paths.get(pfad), lines);
  	} catch (Exception e) {
  		// TODO Auto-generated catch block
  		e.printStackTrace();
  	}
   
  }
}