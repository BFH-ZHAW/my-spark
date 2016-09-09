package com.bruttel.actus;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	String path = args[0];
		
		// Create Spark Session	
		   SparkSession sparkSession = SparkSession
		    		.builder()
		    		.appName("com.bruttel.actus.CreateEmptyParquet")
//		    		.enableHiveSupport()
		    		.getOrCreate();

		// Generate the schema 
        StructType eventsSchema = DataTypes
                .createStructType(new StructField[] {
                    DataTypes.createStructField("id", DataTypes.StringType, false),
                    DataTypes.createStructField("date", DataTypes.StringType, false),
                    DataTypes.createStructField("type", DataTypes.StringType, false),
                    DataTypes.createStructField("currency", DataTypes.StringType, false),
                    DataTypes.createStructField("value", DataTypes.DoubleType, false),
                    DataTypes.createStructField("nominal", DataTypes.DoubleType, false),
                    DataTypes.createStructField("accrued", DataTypes.DoubleType, false)});
		 
			//Create Portfolio Data Frame from CSV	
			Dataset<Row> dsEvents = sparkSession.read()
		            .option("header", "true")
					.option("sep", ",")
				    .schema(eventsSchema)
				    .csv(path.concat("events.csv"))    // Equivalent to format("csv").load("/path/to/directory")
				    .cache();
		
			//Register as table to use sql later and show it
			dsEvents.createOrReplaceTempView("events");
	    	System.out.println("dsEvents geladen");
	    	dsEvents.printSchema();
	    	dsEvents.show();   

			//SQL query with no sucess to get no files back
			String sqlquery = "SELECT * FROM events";
//						+ "WHERE id = 'different'";
// Folgendes geht nicht, weil INSERT nicht unterstützt wird:
//			String sqlquery2 = "INSERT INTO TABLE events VALUES ('CT1','2016-01-01T00:00','ADO','EUR',0.0,48000.0,0.0)";
			
	    	//SQL Query ausführen
	    	Dataset<Row> dsResult = dsEvents.sqlContext().sql(sqlquery).cache();
	    	//dsResult = dsEvents.sqlContext().sql(sqlquery2).cache();
	    	
			// show new table
	    	System.out.println("dsResult leer");
	    	dsResult.printSchema();
	    	dsResult.show();  
	    	
	    	 // DataFrames can be saved as Parquet files, maintaining the schema information.
	    	dsResult.write().mode("append").csv(path.concat("empty.csv"));    	
	}

}
