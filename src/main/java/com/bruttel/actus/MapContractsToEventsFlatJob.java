package com.bruttel.actus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.actus.conversion.DateConverter;

import javax.time.calendar.ZonedDateTime;
import scala.Tuple2;
import java.util.Arrays;

public class MapContractsToEventsFlatJob {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }
    //Input Parameter:
    String contractsFile = args[0]; //hdfs://160.85.30.40/user/spark/data/contracts_10000000.csv 
    String riskfactorsFile = args[1]; //hdfs://160.85.30.40/user/spark/data/riskfactors_input.csv 
    String timespecsFile = args[2]; //hdfs://160.85.30.40/user/spark/data/timespecs_input.csv 
    String outputPath = args[3]; //hdfs://160.85.30.40/user/spark/data/output/; 
    //String way = args[3];  //count or Group by
    String debug = args[4];

  
    //Create Spark Session
    SparkSession sparkSession = SparkSession
    		.builder()
    		.appName("sparkjobs.MapContractsToEventsFlatJob")

    		.getOrCreate();

    
    // for time stopping
    long start = System.currentTimeMillis();
     
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
    
    // import risk factor data, map to connector and broadcast
    JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsFile).javaRDD(); // contract data
    JavaPairRDD<String, String[]> riskFactorRDD = riskFactor.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));
   
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(riskFactorRDD.first());
    }
    
	// import and map contract data to contract event results
    JavaRDD<String> contractFile = sparkSession.read().textFile(contractsFile).javaRDD(); // contract data
    
    // Create DataFrame Schema
    StructType eventsSchema = DataTypes
            .createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("date", DataTypes.StringType, false),
                DataTypes.createStructField("type", DataTypes.StringType, false),
                DataTypes.createStructField("currency", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false),
                DataTypes.createStructField("nominal", DataTypes.DoubleType, false),
                DataTypes.createStructField("accrued", DataTypes.DoubleType, false)
                });

    
    //Flatmap ausführen und Data Frame erstellen
	JavaRDD<Row> events = contractFile.flatMap(new ContractToEventsFlatFunction(_t0, riskFactorRDD.collectAsMap()));
	Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema).cache();

	//For SQL Querying:
 	//cachedEvents.registerTempTable("events");
    cachedEvents.createOrReplaceTempView("events");
    

    //Debug Info
    if(debug.equals("debug")){
    	System.out.println("cachedEvents Schema und show");
	    cachedEvents.printSchema();
	    cachedEvents.show();   
	   
    }
    
    // DataFrames can be saved as Parquet files, maintaining the schema information.
	cachedEvents.write().parquet(outputPath + "events.parquet");
 	
//Data is now ready and it's possible to query the data:
 	    
// 	results.registerTempTable("events");
// 	DataFrame dfCount = 	results
//				.sqlContext().sql("SELECT COUNT(*) "
//								+ "FROM events ");
// 	dfCount.show();
     
    // stop spark context
    sparkSession.stop();
    
    // print time measurements
     // System.out.println(way);
    System.out.println("stopped time in Sec.: " + (System.currentTimeMillis()-start)/1000);
  }
}