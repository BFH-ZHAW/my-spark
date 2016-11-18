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


public class MapConToEvExtFlatJobV2 {

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
    String ram =args[7]; //12GB wird f�rs Log Gebraucht
    //String size =args[6]; //3GB wird f�rs Log gebraucht
    String run =args[8]; //1-10 wird f�rs Log gebraucht
    String knoten =args[9]; //1-8 wird f�rs Log gebraucht
    
    String outputPath = path.concat("output/");
    String contractsPath = path.concat(contracts); //contracts_10000000.csv 
    String riskfactorsPath = path.concat(riskfactors); //riskfactors_input.csv
    
    //typically you want 2-4 partitions for each CPU -> Each Node has 2 Cores
    int partitions = Integer.parseInt(knoten)*3*2;
    
    //Klassenname wird wieder verwendet:    
    String className = "com.bruttel.actus.MapConToEvExtFlatJobV2";

    //Create Spark Session
    SparkSession sparkSession = SparkSession
    		.builder()
    		.appName(className)
    		.getOrCreate();

    // for time stopping
    long start = System.currentTimeMillis();
    
    SparkContext sparkContext = sparkSession.sparkContext(); 
    
    //sparkSession.sparkContext().broadcast(value, evidence$11)
     
    // import and broadcast analysis date als Dataframe
   	Dataset<Row> timespecsFile = sparkSession.read().option("header", false).csv(timespecsPath);
   //	timespecsFile.first().getString(0);
//    JavaRDD<String> timeSpecs = sparkSession.read().textFile(timespecsFile).javaRDD(); // analysis time specification
//    JavaRDD<String> timeVector = timeSpecs.flatMap(line -> Arrays.asList(line.split(";")).iterator());
    ZonedDateTime _t0 = null;
    try{
    	//_t0 = DateConverter.of(timeVector.first());  
    	_t0 = DateConverter.of(timespecsFile.first().getString(0));  	
    } catch(Exception e) {
      System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
    }
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(_t0);
    }
//    Broadcast<ZonedDateTime> t0 = sparkContext.broadcast(_t0);
    
    // import risk factor data as Dataframe and count
   	Dataset<Row> riskfactorsFile = sparkSession.read().option("header", true).option("sep",";").csv(riskfactorsPath);    
   	long riskfactorsAmount = riskfactorsFile.count()/4;
   	
    if(debug.equals("debug")){
    	System.out.println("riskfactorsFile printSchema() und show() von Total:"+riskfactorsAmount+" Risikoszenarien");
    	riskfactorsFile.printSchema();
    	riskfactorsFile.show();  
    }
   	
   	// import contract data as Dataframe and count 
   	Dataset<Row> contractsFile = sparkSession.read().option("header", true).option("sep",";").csv(contractsPath);
   	long contractsAmount = contractsFile.count();
   	
    if(debug.equals("debug")){
    	System.out.println("contractsFile printSchema() und show() von Total:"+contractsAmount+" Contracts");
    	contractsFile.printSchema();
    	contractsFile.show();  
    } 
    
    //Join der Files    
    Dataset<Row> filesJoin = contractsFile.join(riskfactorsFile, riskfactorsFile.col("MarketObjectCode").equalTo(contractsFile.col("marketObjectCodeRateReset")));
    long joinAmount = filesJoin.count();
    
    if(debug.equals("debug")){
    	System.out.println("FilesJoin printSchema() und show() von Total:"+joinAmount+" Contracts");
    	filesJoin.printSchema();
    	filesJoin.show();  
    	System.out.println("filesJoin.javaRDD().first().getString(0)");
    	System.out.println(filesJoin.javaRDD().first().getString(0));
    	System.out.println(filesJoin.javaRDD().first().length());
    	System.out.println(filesJoin.javaRDD().first());
    	System.out.println(filesJoin.javaRDD().first().size());
    	System.out.println(filesJoin.javaRDD().first().schema());
    	System.out.println(filesJoin.javaRDD().first().get(0));
		System.out.println(filesJoin.javaRDD().first().getString(4));
        System.out.println(filesJoin.javaRDD().first().getString(5));
        System.out.println(filesJoin.javaRDD().first().getString(7));
        System.out.println(filesJoin.javaRDD().first().getString(7));
        System.out.println(filesJoin.javaRDD().first().getString(10));
        System.out.println("Problem maybe here");
        System.out.println(Double.parseDouble(filesJoin.javaRDD().first().getString(11)));
        System.out.println(filesJoin.javaRDD().first().getString(12));
        System.out.println(filesJoin.javaRDD().first().getString(16));
        System.out.println(filesJoin.javaRDD().first().getString(17));
        System.out.println(filesJoin.javaRDD().first().getString(18));
        System.out.println(filesJoin.javaRDD().first().getString(19));
        System.out.println(Double.parseDouble(filesJoin.javaRDD().first().getString(20)));
        System.out.println(filesJoin.javaRDD().first().getString(21));
        System.out.println(Double.parseDouble(filesJoin.javaRDD().first().getString(22)));
        System.out.println(filesJoin.javaRDD().first().getString(23));
        System.out.println(Double.parseDouble(filesJoin.javaRDD().first().getString(24)));
        System.out.println(filesJoin.javaRDD().first().getString(25));
        System.out.println(Double.parseDouble(filesJoin.javaRDD().first().getString(26)));
        System.out.println(filesJoin.javaRDD().first().getString(30));
        System.out.println(filesJoin.javaRDD().first().getString(31));
        System.out.println(Double.parseDouble(filesJoin.javaRDD().first().getString(22)));
        System.out.println(filesJoin.javaRDD().first().getString(33));
        System.out.println(filesJoin.javaRDD().first().getString(34));
        System.out.println(filesJoin.javaRDD().first().getString(35));
        System.out.println(Double.parseDouble(filesJoin.javaRDD().first().getString(36)));
        System.out.println(Double.parseDouble(filesJoin.javaRDD().first().getString(37)));
        System.out.println(filesJoin.javaRDD().first().getString(38));
        System.out.println(Double.parseDouble(filesJoin.javaRDD().first().getString(39)));
        System.out.println(filesJoin.javaRDD().first().getString(47));
        System.out.println(filesJoin.javaRDD().first().getString(48));
        System.out.println(filesJoin.javaRDD().first().getString(49));
        System.out.println(filesJoin.javaRDD().first().getString(45));  	
    	
    } 
    
    JavaRDD<Row> events = filesJoin.javaRDD().flatMap(new ContToEventFunc(_t0));
    
    
   	
////    JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsFile).javaRDD(); // risiko data
////    JavaPairRDD<String, String[]> riskFactorRDD = riskFactor.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));
//    //Check if Chache is useful:
//   	riskFactor.count();
//  //  int riskamount = Integer.parseInt(riskfactors.replace("riskfactors_", "").replace(".csv", ""));
//    if(riskamount <= 1000 ){
//    	riskFactorRDD.cache();}
//    else {
//    	riskFactorRDD.repartition(partitions);}
//       
//	// import contracts data, map to connector
//    JavaRDD<String> contractFile = sparkSession.read().textFile(contractsFile).javaRDD(); // contract data
//    JavaPairRDD<String, String[]> contractFileRDD = contractFile.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[40], temp.split(";")));
//    //Check if Broadcast is useful:
//    int contractamount = Integer.parseInt(contracts.replace("contracts_", "").replace(".csv", ""));
//    if(contractamount <= 1000 ){
//    	contractFileRDD.cache();}
//    else {
//    	contractFileRDD.repartition(partitions);}
//    
//    JavaPairRDD<String, Tuple2<String[], String[]>> contractsAndRisk = contractFileRDD.join(riskFactorRDD);
//	JavaRDD<Row> events = contractsAndRisk.values().flatMap(new ContToEvExtFlatFunc(_t0));
	
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