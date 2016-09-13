package com.bruttel.actus;

import java.util.ArrayList;

//Basic map example in Java 8

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

public class FlatMapExtendedExample {
 public static void main(String[] args) throws Exception {
	 SparkSession sparkSession = SparkSession
	    		.builder()
	    		.appName("com.bruttel.actus.FlatMapExtendedExample")
	    		.getOrCreate();
     
	 //Liste Risiko erstellen
	 List<String> riskList = Arrays.asList("crash", "death", "strocke", "explosion", "war");
     Dataset<String> riskDataset = sparkSession.createDataset(riskList, Encoders.STRING());
     
     //Liste Contracts erstellen
	 List<String> contractsList = Arrays.asList("C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10");
     Dataset<String> contractsDataset = sparkSession.createDataset(contractsList, Encoders.STRING());  
     JavaRDD<String> contractsJavaRDDString = contractsDataset.toJavaRDD();

     //Struct erstellen:
     StructType contractsSchema = DataTypes.createStructType(new StructField[]{
								    		 DataTypes.createStructField("id", DataTypes.StringType, false),
								             DataTypes.createStructField("date", DataTypes.StringType, false)});
    
//     JavaRDD<Row> contracts = datasetContracts.flatMap(new mapContractsFunction(datasetRisk));
     JavaRDD<Row> contractsJavaRDDRow = contractsJavaRDDString.flatMap(new mapContractsFunction(riskDataset));
     Dataset<Row> contractDatasetRow = sparkSession.createDataFrame(contractsJavaRDDRow, contractsSchema).cache();
     
//	Ausgabe:
     contractDatasetRow.schema();
     contractDatasetRow.show();     
     
 }
}

//Klasse mapContractsFunction erstellen (implements includiert die FlatMapFunktion. Erster Parameter ist eingangswert, zweiter der Ausgangswert)
@SuppressWarnings("serial")
class mapContractsFunction implements FlatMapFunction<String,Row> {
	
	//Wird für das XxY Mapping verwendet
	Dataset<String> datasetRisk;
	
	//Aufruf der Funktion (DatasetRisk wird als Konstruktor mitgegeben) 
	public mapContractsFunction(Dataset<String> datasetRisk){
		this.datasetRisk = datasetRisk;
	}
	
	//Dieser Aufruf (call String) wird von der FlatMapFunction gebraucht, hier wird festgelegt, wie der Input verarbeitet wird.
	@Override
	public Iterator<Row> call(String contractsList) throws Exception {
		  //Ausgangsliste erstellen (Ausgabe ist Row, aber RowList ist mehrfaches von Row und somit bei Flatmap erlaubt)
		  List<Row> rowList = new ArrayList<>();
		  datasetRisk.foreach(risiko -> rowList.add(RowFactory.create(contractsList, risiko)));	  
		  return rowList.iterator();

	}
}