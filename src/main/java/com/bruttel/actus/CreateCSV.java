package com.bruttel.actus;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;

public class CreateCSV {
	// Create a BufferedWriter around a FileWriter.
	  public static void main(String[] args) throws IOException, Exception, URISyntaxException   {
			// Check if Amount of Arguments is correct. 
		  if (args.length != 3) {
			      throw new IOException("Usage: Path Amount");
					}
		  //Pass Arguments
		  		String target = args[0];
		  		String path = args[1];
			    String amountString = args[2];
			    Long amount;
			    //Try and Assing Argument 2 as Integer
			    try { amount = Long.parseLong(amountString);
				} catch (NumberFormatException e) {
				    System.out.println("No Number, therefore 10 000");
				   amount = (long) 10000;				
			}
		
			if ( target.equals("hdfs"))  {
		    //Wenn der Parameter HDFS ist wird dieser Teil ausgeführt: 
				
				//HDFS Config
				org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
				configuration.addResource(new org.apache.hadoop.fs.Path("/home/user/hadoop-2.7.2/etc/hadoop/core-site.xml")); 
				//HDFS Filesystem definieren
			    FileSystem hdfs = FileSystem.get(URI.create( "hdfs://160.85.30.40" ), configuration );			    
			    
			    System.out.println("Connecting to -- "+configuration.get("hdfs.defaultFS"));
			    
			    //HDFS Pfad konfigurieren
			    org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path("hdfs://160.85.30.40/user/spark/data/"+path);
			    //Falls das File bereits exisitert wird es gelöscht. 
			    if ( hdfs.exists( file )) { 
			    	hdfs.delete( file, true ); 
			    	}
			    // Ab hier beginnt die Zeitmessung:
			    long startTime = System.currentTimeMillis();
				//Buffered Writer erstellen
			    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(file,true)));
			    
				// Write Header:Nr. ,Portfolio,Nennwert,Laufzeit,Zins
				writer.write("Nr,Portfolio,Nennwert,Laufzeit,Zins");
				writer.newLine();
				
				// While File is smaller than amount given as parameter in MB (Byte * 1024 = KB *1024 = MB)
				int j = 0; // fÃ¼r den Index

				while ( amount*1024*1024 > hdfs.getFileStatus(file).getLen()){
					// For Schleife erstellt 10'000 neue Records	
					for(int i= 1; i < 10000; i++){

						writer.write(i+j+","+ //Line ID
									(new Random().nextInt(10)+1)+","+ //Portfolionummern von 1-10
									(new Random().nextInt(1000)+1000)+","+ //Nennwerte von 1000 - 1'000'000
									(new Random().nextInt(5)+1)+","+ //Laufzeit von 1-5 Jahre
									"1."+(new Random().nextInt(9))); //Zins von 1.0 - 1.9 % 
						writer.newLine();
					}
					j = j + 10000;
				}
				
				
				//Ende der Zeitmessung:
				long endTime = System.currentTimeMillis();
				
				writer.close();
				hdfs.close();
				
				//Informationen ausgeben
				System.out.println("File mit "+j+10000+" Zeilen geschrieben.");
				System.out.println("File Grösse: "+(hdfs.getFileStatus(file).getLen()/1024/1024)+" MB");
				System.out.println("Dauer: "+((endTime-startTime)/1000)+" sec");
				System.out.println("Ziel: "+target);
				System.out.println("WAS?");
							   
			}
			else {
				File file = new File(path);   
				if ( file.exists()) {
					file.delete();
					}
		    
			    // Ab hier beginnt die Zeitmessung:
			    long startTime = System.currentTimeMillis();
				    
				// ... Write to an output text file.
				BufferedWriter writer = new BufferedWriter(new FileWriter( path ));
				// Write Header:Nr. ,Portfolio,Nennwert,Laufzeit,Zins
				writer.write("Nr,Portfolio,Nennwert,Laufzeit,Zins");
				writer.newLine();
				
				// While File is smaller than amount given as parameter in MB (Byte * 1024 = KB *1024 = MB)
				int j = 0; // fÃ¼r den Index
				while ( amount*1024*1024 > file.length()){
					// For Schleife erstellt 10'000 neue Records	
					for(int i= 1; i < 10000; i++){
	
						writer.write(i+j+","+ //Line ID
									(new Random().nextInt(10)+1)+","+ //Portfolionummern von 1-10
									(new Random().nextInt(1000)+1000)+","+ //Nennwerte von 1000 - 1'000'000
									(new Random().nextInt(5)+1)+","+ //Laufzeit von 1-5 Jahre
									"1."+(new Random().nextInt(9))); //Zins von 1.0 - 1.9 % 
						writer.newLine();
					}
					j = j + 10000;
				}
				
				
				//Ende der Zeitmessung:
				long endTime = System.currentTimeMillis();
				
				writer.close();
				
				//Informationen ausgeben
				System.out.println("File mit "+j+10000+" Zeilen geschrieben.");
				System.out.println("File Grösse: "+(file.length()/1024/1024)+" MB");
				System.out.println("Dauer: "+((endTime-startTime)/1000)+" sec");
				System.out.println("Ziel: "+target);
			}
		  
	  }
	
}
