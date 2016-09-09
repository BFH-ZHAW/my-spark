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
				    .csv("hdfs://160.85.30.40/user/events.csv")    // Equivalent to format("csv").load("/path/to/directory")
				    .cache();
		
			//Register as table to use sql later and show it
			dsEvents.createOrReplaceTempView("events");
	    	System.out.println("dsEvents geladen");
	    	dsEvents.printSchema();
	    	dsEvents.show();   

			//SQL query with no sucess to get no files back
			String sqlquery = "SELECT * FROM events";
			String sqlquery2 = "INSERT INTO TABLE events VALUES ('CT1','2016-01-01T00:00','ADO','EUR',0.0,48000.0,0.0)";
//						+ "WHERE id = 'different'";
			
	    	//SQL Query ausführen
	    	Dataset<Row> dsResult = dsEvents.sqlContext().sql(sqlquery).cache();
	    	//dsResult = dsEvents.sqlContext().sql(sqlquery2).cache();
	    	
			// show new table
	    	System.out.println("dsResult leer");
	    	dsResult.printSchema();
	    	dsResult.show();  
	    	
	    	 // DataFrames can be saved as Parquet files, maintaining the schema information.
	    	dsResult.write().parquet("hdfs://160.85.30.40/user/spark2/data/empty.parquet");    	
	    	
//	    	Result:
//	    		dsEvents geladen
//	    		root
//	    		 |-- id: string (nullable = true)
//	    		 |-- date: string (nullable = true)
//	    		 |-- type: string (nullable = true)
//	    		 |-- currency: string (nullable = true)
//	    		 |-- value: double (nullable = true)
//	    		 |-- nominal: double (nullable = true)
//	    		 |-- accrued: double (nullable = true)
//
//	    		+---+----------------+----+--------+-----+-------+-------+
//	    		| id|            date|type|currency|value|nominal|accrued|
//	    		+---+----------------+----+--------+-----+-------+-------+
//	    		|CT1|2016-01-01T00:00| ADO|     EUR|  0.0|48000.0|    0.0|
//	    		+---+----------------+----+--------+-----+-------+-------+
//
//	    		dsResult leer
//	    		root
//	    		 |-- id: string (nullable = true)
//	    		 |-- date: string (nullable = true)
//	    		 |-- type: string (nullable = true)
//	    		 |-- currency: string (nullable = true)
//	    		 |-- value: double (nullable = true)
//	    		 |-- nominal: double (nullable = true)
//	    		 |-- accrued: double (nullable = true)
//
//	    		+---+----+----+--------+-----+-------+-------+
//	    		| id|date|type|currency|value|nominal|accrued|
//	    		+---+----+----+--------+-----+-------+-------+
//	    		+---+----+----+--------+-----+-------+-------+
//
//	    		SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
//	    		SLF4J: Defaulting to no-operation (NOP) logger implementation
//	    		SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
//	    		07.09.2016 00:17:36 INFORMATION: org.apache.parquet.hadoop.codec.CodecConfig: Compression: SNAPPY
//	    		07.09.2016 00:17:36 INFORMATION: org.apache.parquet.hadoop.ParquetOutputFormat: Parquet block size to 134217728
//	    		07.09.2016 00:17:36 INFORMATION: org.apache.parquet.hadoop.ParquetOutputFormat: Parquet page size to 1048576
//	    		07.09.2016 00:17:36 INFORMATION: org.apache.parquet.hadoop.ParquetOutputFormat: Parquet dictionary page size to 1048576
//	    		07.09.2016 00:17:36 INFORMATION: org.apache.parquet.hadoop.ParquetOutputFormat: Dictionary is on
//	    		07.09.2016 00:17:36 INFORMATION: org.apache.parquet.hadoop.ParquetOutputFormat: Validation is off
//	    		07.09.2016 00:17:36 INFORMATION: org.apache.parquet.hadoop.ParquetOutputFormat: Writer version is: PARQUET_1_0
//	    		07.09.2016 00:17:36 INFORMATION: org.apache.parquet.hadoop.InternalParquetRecordWriter: Flushing mem columnStore to file. allocated memory: 0

	}

}

