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

//    // Create Spark Context (Old Version)
//    SparkConf conf = new SparkConf().setAppName("sparkjobs.MapContractsToEventsJob");//.setMaster("local");
//    JavaSparkContext sparkSession = new JavaSparkContext(conf);
//    // Create SQL Context 
//    SQLContext sqlContext = new SQLContext(sparkSession);
   
    //Create Spark Session
    SparkSession sparkSession = SparkSession
    		.builder()
    		.appName("sparkjobs.MapContractsToEventsJob")
 //Funktioniert leider nicht, weil die Hive Klassen nicht gefunden wurden.  
 //  		.enableHiveSupport()
    		.getOrCreate();
    //Create SQL Context -> obsolet?
    //SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkSession);
    
    // for time stopping
    long start = System.currentTimeMillis();
     
    // import and broadcast analysis date
    JavaRDD<String> timeSpecs = sparkSession.read().textFile(timespecsFile).javaRDD(); // analysis time specification
    JavaRDD<String> timeVector = timeSpecs.flatMap(line -> Arrays.asList(line.split(";")).iterator());
//    String[] timeVector = timeSpecsLine.first();
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
    //Broadcast<ZonedDateTime> t0 = sparkSession.broadcast(_t0);
    
    // import risk factor data, map to connector and broadcast
    JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsFile).javaRDD(); // contract data
    JavaPairRDD<String, String[]> riskFactorRDD = riskFactor.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));
//    JavaPairRDD<String, String[]> riskFactorRDD = riskFactor.mapToPair(
//      new PairFunction<String, String, String[]>() {
//        public Tuple2<String, String[]> call(String s) {
//          String[] temp = s.split(";");
//          return new Tuple2(temp[0], temp);
//        }
//      });    
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(riskFactorRDD.first());
    }
    //Broadcast<Map<String,String[]>> riskFactors = riskFactorRDD.collectAsMap();
    
	// import and map contract data to contract event results
    JavaRDD<String> contractFile = sparkSession.read().textFile(contractsFile).javaRDD(); // contract data
//    JavaRDD<Row> events = contractFile.map(new ContractToEventsWriteFunction(_t0, riskFactorRDD.collectAsMap(), sparkSession, outputPath));
   // JavaRDD<Row> events = contractFile.map(new ContractToEventsFunction(t0, riskFactors));
    
    
//   // Jede Zeile Verarbeiten:
//    JavaRDD<Row> eventsflat = events.flatMap(new FlatMapFunction<Row, Row>() {
//        @Override
//        public Iterator<Row> call(Row row) throws Exception {
//          //Dynamische Gr�sse der Arrays pro Zeile:
//          int size =Array.getLength((String[]) row.get(0));
//          
//          //Ausgabefile erstellen
//          JavaRDD<Row> Zeilen; 
//          
//          //eine Ziele pro Ausgabe
//          for (int i=0; i < Array.getLength((String[]) row.get(0)); i++){
//            Zeilen.union(  row.get(0)[i] //"id",
//            			+";"+ row.get(1)[i] // "date"
//            			+";"+ row.get(2)[i] // "type"
//            			+";"+ row.get(3)[i] // "currency"
//            			+";"+ (String) row.get(4)[i] // "value"
//            			+";"+ (String) row.get(5)[i] // "nominal"
//            			+";"+ (String) row.get(6)[i] // ("accrued"
//            			);
//          }
//          
//          return Zeilen;
//        }
//      });
  
//    JavaRDD<Row> eventsflat = events.flatMap(zeile -> 
//
//    	JavaRDD<String> words =
//    		    lines.flatMap(line -> Arrays.asList(line.split(" ")));
//    		JavaPairRDD<String, Integer> counts =
//    		    words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
//    		         .reduceByKey((x, y) -> x + y);
//    		counts.saveAsTextFile("hdfs://counts.txt");
    	
    	
    

    
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
   
//Leeres Data Frame erstellen hat leider nicht funktioniert:
//    Dataset<Row> cachedEventsLeer1 = sparkSession.emptyDataFrame();
    //sparkSession.emptyDataset();
//    Encoder<Row> rowEncoder = Encoders.tuple(STRING(), STRING() ,STRING() , STRING() , DOUBLE(), DOUBLE(), DOUBLE());
//    Dataset<Row> cachedEventsLeer2 = null;
//    try {
    	//Mit Lerem Rwo element
//    JavaRDD<Row> row = null ;
//    Dataset<Row> cachedEventsLeer = sparkSession.createDataFrame( row, eventsSchema);
//    	//Direkt Leeres DataFrame
//    cachedEventsLeer2 = sparkSession.emptyDataFrame();
//    cachedEventsLeer2.schema().add("id", DataTypes.StringType)
//    						  .add("date", DataTypes.StringType)
//    						  .add("type", DataTypes.StringType)
//    						  .add("currency", DataTypes.StringType)
//    						  .add("value", DataTypes.DoubleType)
//    						  .add("nominal", DataTypes.DoubleType)
//    						  .add("accured", DataTypes.DoubleType);
//    System.out.println("cachedEventsSqeuentiellSchema:");
//    cachedEventsLeer2.printSchema();
//    	//TempView erstellen
//    cachedEventsLeer2.createOrReplaceTempView("eventsSeq");
//
//    
//    }
//    catch(Exception e) {
//        System.out.println(e.getClass().getName() + " when creating a new empty Dataset eventsSeq");
//      }
//    Dataset<Row> cachedEventsLeer = sparkSession.createDataFrame( sparkSession.emptyDataset(row),eventsSchema);
//     Dataset<Row> cachedEventsLeer = sparkSession.emptyDataFrame();
//     Dataset<Row> cachedEventsLeer2 = applySchema(cachedEventsLeer, eventsSchema);
   // Dataset<Row> test =  createDataFrame(new java.util.List<Row>()  rows,eventsSchema);
  //  JavaRDD<Row> empty1 = [id: int, name: string];
//    JavaRDD<Row> empty = sparkSession.createDataFrame(empty1,eventsSchema);
//    Dataset<Row> cachedEvents = sparkSession.createDataFrame(emptyRDD(), eventsSchema).cache();
//       Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema).cache();
    
//Zuvor erstelltes Leeres Dataframe laden, welches bereits das korrekte Schema enth�lt:
    Dataset<Row> cachedEventsLeer2 = sparkSession.read().parquet("hdfs://160.85.30.40/user/spark2/data/empty.parquet");
    cachedEventsLeer2.createOrReplaceTempView("eventsSeq");
//    cachedEventsLeer2.write().mode("overwrite").saveAsTable("evetnsSeqTable");
    
//Nun wieder den bisherigen Code ausf�hren
   JavaRDD<Row> events = contractFile.map(new ContractToEventsFlatFunction(_t0, riskFactorRDD.collectAsMap(), sparkSession, outputPath));
   Dataset<Row> cachedEvents = sparkSession.createDataFrame(events, eventsSchema).cache();
    //For SQL Querying:
 	//cachedEvents.registerTempTable("events");
    cachedEvents.createOrReplaceTempView("events");
    
//    sparkSession.sql(" INSERT INTO TABLE eventsSeq VALUES('CT1','2016-01-01T00:00','ADO','EUR',0.0,48000.0,0.0)");

    //Debug Info
    if(debug.equals("debug")){
    	System.out.println("cachedEventsOriginal");
	    cachedEvents.printSchema();
	    cachedEvents.show();   
	    System.out.println("cachedEventsSqeuentiell");
	    cachedEventsLeer2.printSchema();
	    cachedEventsLeer2.show();   
//	    System.out.println("cachedEventsSaveTable");
//	    sparkSession.table("eventsSeqTable").printSchema();
//	    sparkSession.table("eventsSeqTable").show();   	    
	    //Das hat leider auch nicht funktioniert:
//	    System.out.println("Tabelle per SQL erstellen:");
//	    sparkSession.sql("CREATE TABLE IF NOT EXISTS eventsSQL (id INT, date STRING)");
//	    sparkSession.sql("INSERT INTO TABLE eventsSQL VALUES (1, '09.09.2016')");
//	    Dataset<Row> cachedEventsLeer3 = sparkSession.sql("SELECT * FROM eventsSQL");
//	    cachedEventsLeer3.printSchema();
//	    cachedEventsLeer3.show();  
    }
    
 // DataFrames can be saved as Parquet files, maintaining the schema information.
 //	cachedEvents.write().parquet(outputPath + "events.parquet");
 	
//Data is now ready and it's possible to query the data:
 	 
    
// 	results.registerTempTable("events");
// 	DataFrame dfCount = 	results
//				.sqlContext().sql("SELECT COUNT(*) "
//								+ "FROM events ");
// 	dfCount.show();
     
 	 
 	 // This is not necessary any more
// 	// count records or save file
//    if (way.equals("count")){
//    	results.registerTempTable("events");
//    	DataFrame dfCount = 	results
//				.sqlContext().sql("SELECT COUNT(*) "
//								+ "FROM events ");
//    	dfCount.show();
//    }
//    else {
// 	 results.javaRDD().saveAsTextFile(outputPath);
//    }
    
    
    
    // stop spark context
    sparkSession.stop();
    
    // print time measurements
     // System.out.println(way);
    System.out.println("stopped time in Sec.: " + (System.currentTimeMillis()-start)/1000);
  }
}