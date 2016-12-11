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
    //Braucht es nicht: 
	 //Dataset<String> riskDataset = sparkSession.createDataset(riskList, Encoders.STRING());
	 
     
     //Liste Contracts erstellen
	 List<String> contractsList = Arrays.asList("C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10");
     Dataset<String> contractsDataset = sparkSession.createDataset(contractsList, Encoders.STRING());  
     JavaRDD<String> contractsJavaRDDString = contractsDataset.toJavaRDD();
     
//     //TEST
//     List<Row> rowList = new ArrayList<>();
//     contractsJavaRDDString.foreach(contract -> rowList.add(RowFactory.create(contract))); 
//     System.out.println("rowList.size(): "+rowList.size());
//    // System.out.println("rowList.get(1): "+rowList.get(1));
//     riskList.
//     rowList.forEach(e -> System.out.println("im FooreachLoop"+e));
//     for (int i=0; i < contractsJavaRDDString.count() ; i++){
//    	 System.out.println("riskDataset.take(i)"+contractsJavaRDDString.take(i));
//    	 rowList.add(RowFactory.create(contractsJavaRDDString.take(i)));
//     }
//     System.out.println("rowList.size(): "+rowList.size());
     
     //Struct erstellen:
     StructType contractsSchema = DataTypes.createStructType(new StructField[]{
								    		 DataTypes.createStructField("id", DataTypes.StringType, false),
								             DataTypes.createStructField("date", DataTypes.StringType, false)});   
     
     //Data Frame erstellen, in dem die Custom Function aufgerufen wird. 
     JavaRDD<Row> contractsJavaRDDRow = contractsJavaRDDString.flatMap(new mapContractsFunction(riskList));
     Dataset<Row> contractDatasetRow = sparkSession.createDataFrame(contractsJavaRDDRow, contractsSchema).cache();
     
     //Ausgabe:
     contractDatasetRow.schema();
     contractDatasetRow.show();     
     
 }
}

//Klasse mapContractsFunction erstellen (implements includiert die FlatMapFunktion. Erster Parameter ist eingangswert, zweiter der Ausgangswert)
@SuppressWarnings("serial")
class mapContractsFunction implements FlatMapFunction<String,Row> {
	
	//Wird für das XxY Mapping verwendet
	List<String> riskList;
	
	//Aufruf der Funktion (DatasetRisk wird als Konstruktor mitgegeben) 
	public mapContractsFunction(List<String> riskList){
		this.riskList = riskList;
	}
	
	//Dieser Aufruf (call String) wird von der FlatMapFunction gebraucht, hier wird festgelegt, wie der Input verarbeitet wird.
	@Override
	public Iterator<Row> call(String contract) throws Exception {

		  //Ausgangsliste erstellen (Ausgabe ist Row, aber RowList ist mehrfaches von Row und somit bei Flatmap erlaubt)
		  List<Row> rowList = new ArrayList<>();
		  
		  //Hier wird das zweite Array jeweils zugeteilt. 
		  for (int i=0; i < riskList.size(); i++){
		  		rowList.add(RowFactory.create(contract, riskList.get(i)));
		  }
		  //Das wäre viel eleganter, funktioniert aber nicht weil?!?!?		
		  // datasetRisk.foreach(risiko -> rowList.add(RowFactory.create(contract, risiko)));	  
		  return rowList.iterator();

	}
}