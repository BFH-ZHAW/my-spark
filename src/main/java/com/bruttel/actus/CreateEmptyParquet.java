package com.bruttel.actus;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateEmptyParquet {

	public static void main(String[] args) {
		// Create Spark Session	
		   SparkSession sparkSession = SparkSession
		    		.builder()
		    		.appName("com.bruttel.actus.CreateEmptyParquet")
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
				    .csv("/home/user/events.csv")    // Equivalent to format("csv").load("/path/to/directory")
				    .cache();
		
			//Register as table to use sql later and show it
			dsEvents.createOrReplaceTempView("events");
	    	System.out.println("dsEvents geladen");
	    	dsEvents.printSchema();
	    	dsEvents.show();   

			//SQL query
			String sqlquery = "DELETE FROM events";
//						+ "Where ...";
			
	    	//SQL Query ausführen
	    	Dataset<Row> dsResult = dsEvents.sqlContext().sql(sqlquery).cache();
	    	
			// show new table
	    	System.out.println("dsResult leer");
	    	dsResult.printSchema();
	    	dsResult.show();  
	    	
	    	 // DataFrames can be saved as Parquet files, maintaining the schema information.
	    	dsResult.write().parquet("hdfs://160.85.30.40/user/spark/data/empty.parquet");    	
	}

}

