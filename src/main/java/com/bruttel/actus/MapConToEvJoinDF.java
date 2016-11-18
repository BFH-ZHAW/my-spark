package com.bruttel.actus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.actus.conversion.DateConverter;

import javax.time.calendar.ZonedDateTime;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


public class MapConToEvJoinDF {

  public static void main(String[] args) {
    if (args.length != 10) {
      System.err.println("Usage: path contractFile riskfactorsFile timespecsFile logFile output debug ram run knoten");
      System.exit(0);
    }
    //Input Parameter:
    String path = args[0]; //hdfs://160.85.30.40/user/spark/data/
    String contracts = args[1]; //contracts_10000000.csv 
    String riskfactors = args[2]; //riskfactors_input.csv 
    String timespecsPath = path.concat(args[3]); //timespecs_input.csv 
    String logFile = args[4]; //timespecs_input.csv 
    String output = args[5]; // parquet oder CSV

    String debug = args[6];  //write debug to debug or anything else to not debug
    //String logPath =args[5]; //hdfs://160.85.30.40/user/spark/data/ Hier wird das Logfile geschrieben
    String ram =args[7]; //12GB wird fürs Log Gebraucht
    //String size =args[6]; //3GB wird fürs Log gebraucht
    String run =args[8]; //1-10 wird fürs Log gebraucht
    String knoten =args[9]; //1-8 wird fürs Log gebraucht
    
    String outputPath = path.concat("output/");
    String contractsPath = path.concat(contracts); //contracts_10000000.csv 
    String riskfactorsPath = path.concat(riskfactors); //riskfactors_input.csv
    
    //typically you want 2-4 partitions for each CPU -> Each Node has 2 Cores
    int partitions = Integer.parseInt(knoten)*3*2;
    
    //Klassenname wird wieder verwendet:    
    String className = "com.bruttel.actus.MapConToEvJoinDF";

    //Create Spark Session
    SparkSession sparkSession = SparkSession
    		.builder()
    		.appName(className)
    		.getOrCreate();

    // for time stopping
    long start = System.currentTimeMillis();
         
    // import risk factor data as Dataframe and count
   	Dataset<Row> riskfactorsFile = sparkSession.read().option("header", true).option("sep",";").csv(riskfactorsPath);    

   	// import contract data as Dataframe and count 
   	Dataset<Row> contractsFile = sparkSession.read().option("header", true).option("sep",";").csv(contractsPath);
   	
    //Join der Files    
    Dataset<Row> filesJoin = contractsFile.join(riskfactorsFile, riskfactorsFile.col("MarketObjectCode").equalTo(contractsFile.col("marketObjectCodeRateReset")));

    //Debug Info
    if(debug.equals("debug")){
    	System.out.println("cachedEvents Schema und show");
    	filesJoin.printSchema();
    	filesJoin.show();   
    }
    
    // DataFrames can be saved as Parquet files, maintaining the schema information.  
    if(output.equals("parquet")){
    	filesJoin.write().parquet(outputPath + "joinDF.parquet");
    }
    else {
    	filesJoin.write().csv(outputPath + "joinDF.csv");
    }
	
	//Ende der Zeitmessung:
    long stop = System.currentTimeMillis();

	// stop spark Session
  sparkSession.stop();
  
  //Alternativ Logging to Filestore
  //Hier wird die Zeit für das Logging weggeschrieben. 
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