package com.bruttel.actus;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
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


public class MapConToEvExtFlatJobV4 {

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
    String className = "com.bruttel.actus.MapConToEvExtFlatJobV4";

    //Create Spark Session
    SparkSession sparkSession = SparkSession
    		.builder()
    		.appName(className)
    		.getOrCreate();
    
    // SparkContext -> wird für Broadcast gebraucht
    JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

    // for time stopping
    long start = System.currentTimeMillis();
     
    // import and broadcast analysis date als Dataframe
   	Dataset<Row> timespecsFile = sparkSession.read().option("header", false).csv(timespecsPath);
    ZonedDateTime _t0 = null;
    try{_t0 = DateConverter.of(timespecsFile.first().getString(0));  	
    } catch(Exception e) {
      System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
    }
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(_t0);
    }
    // Broadcast _t0:
    Broadcast<ZonedDateTime> t0 = jsc.broadcast(_t0);
    
    // import risk factor data as Dataframe and count
   	Dataset<Row> riskfactorsFile = sparkSession.read().option("header", true).option("sep",";").csv(riskfactorsPath);    
   	Broadcast<Dataset<Row> > riskfactorsFileBroadcast = jsc.broadcast(riskfactorsFile);
   	//Debug Info
    if(debug.equals("debug")){
    	System.out.println("riskfactorsFile printSchema() und show() von Total:"+riskfactorsFile.count()/4+" Risikoszenarien");
    	riskfactorsFile.printSchema();
    	riskfactorsFile.show();  
    }
   	
   	// import contract data as Dataframe and count 
   	Dataset<Row> contractsFile = sparkSession.read().option("header", true).option("sep",";").csv(contractsPath);
    //	Debug Info   	
    if(debug.equals("debug")){
    	System.out.println("contractsFile printSchema() und show() von Total:");//+contractsAmount+" Contracts");
    	contractsFile.printSchema();
    	contractsFile.show();  
    } 
    
    //Hier wird das Zielformat definiert
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
                DataTypes.createStructField("discont", DataTypes.DoubleType, false),  
                });
    
    //Durch Flatmap werden die Contracts allen Risikofaktoren zugewiesen. 
    JavaRDD<Row> events = contractsFile.javaRDD().flatMap(new ContToEventFuncV4(riskfactorsFileBroadcast.value().collectAsList(), t0.value()));
	//Das Dataframe wird erstellt
    Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema);
    //Debug Information
    if(debug.equals("debug")){
    	System.out.println("cachedEvents Schema und show");
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
    
	//Ende der Zeitmessung:
    long stop = System.currentTimeMillis();
 	
	
	// stop spark Session
    sparkSession.stop();
  
  // Logging to Filestore
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