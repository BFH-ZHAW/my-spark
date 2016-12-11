package com.bruttel.actus;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateEmptyLog {

	public static void main(String[] args) {
		// Create Spark Session	
		   SparkSession sparkSession = SparkSession
		    		.builder()
		    		.appName("com.bruttel.actus.CreateEmptyParquet")
		    		.getOrCreate();

		// Generate the schema 
        StructType eventsSchema = DataTypes
                .createStructType(new StructField[] {
                    DataTypes.createStructField("Klasse", DataTypes.StringType, false),
                    DataTypes.createStructField("RAM", DataTypes.StringType, false),
                    DataTypes.createStructField("Filesize", DataTypes.StringType, false),
                    DataTypes.createStructField("Knoten", DataTypes.StringType, false),
                    DataTypes.createStructField("Run", DataTypes.StringType, false),
                    DataTypes.createStructField("Zeit", DataTypes.StringType, false)
                    });
		 
			//Create Portfolio Data Frame from CSV	
			Dataset<Row> dsEvents = sparkSession.read()
		            .option("header", "true")
					.option("sep", ",")
				    .schema(eventsSchema)
				    .csv("hdfs://160.85.30.40/user/spark/data/log.csv")    // Equivalent to format("csv").load("/path/to/directory")
				    .cache();
		
			//Register as table to use sql later and show it
			dsEvents.createOrReplaceTempView("log");
	    	System.out.println("dsLog geladen");
	    	dsEvents.printSchema();
	    	dsEvents.show();   

			//SQL query with no sucess to get no files back
			String sqlquery = "SELECT * FROM log WHERE Klasse='17'";
//			String sqlquery2 = "INSERT INTO TABLE events VALUES ('CT1','2016-01-01T00:00','ADO','EUR',0.0,48000.0,0.0)";
//						+ "WHERE id = 'different'";
			
	    	//SQL Query ausführen
	    	Dataset<Row> dsResult = dsEvents.sqlContext().sql(sqlquery).cache();
	    	//dsResult = dsEvents.sqlContext().sql(sqlquery2).cache();
	    	
			// show new table
	    	System.out.println("dsResult leer");
	    	dsResult.printSchema();
	    	dsResult.show();  
	    	
	    	 // DataFrames can be saved as Parquet files, maintaining the schema information.
	    	dsResult.write().parquet("hdfs://160.85.30.40/user/spark/data/log");    	
	    	
//	    	Result:
//	    	
	}

}

