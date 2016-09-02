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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.RowFactory;
import org.actus.contracttypes.PrincipalAtMaturity;
import org.actus.models.PrincipalAtMaturityModel;
import org.actus.util.time.EventSeries;
import org.actus.conversion.PeriodConverter;
import org.actus.conversion.DoubleConverter;
import org.actus.contractstates.StateSpace;
import org.actus.riskfactors.RiskFactorConnector;
import org.actus.misc.riskfactormodels.SpotRateCurve;
import org.actus.misc.riskfactormodels.SimpleReferenceRate;
import org.actus.misc.riskfactormodels.SimpleReferenceIndex;
import org.actus.misc.riskfactormodels.SimpleForeignExchangeRate;

import javax.time.calendar.ZonedDateTime;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ContractToEventsWriteFunction implements Function<String,Row> {
//  Broadcast<ZonedDateTime> t0;
//  Broadcast<Map<String,String[]>> riskFactors;
	  ZonedDateTime t0;
	  Map<String,String[]> riskFactors;
	  SparkSession sparkSession;
	  String outputpath;
  
//  public ContractToEventsFunction(Broadcast<ZonedDateTime> t0,
//                                  Broadcast<Map<String,String[]>> riskFactors) {
	  public ContractToEventsWriteFunction(ZonedDateTime t0,
              							   Map<String,String[]> riskFactors,
              							 SparkSession sparkSession,
              							 String oupbutpath) {
   this.t0 = t0;
    this.riskFactors = riskFactors;
    this.sparkSession = sparkSession;
    this.outputpath = outputpath;
    
 }
  
    private static PrincipalAtMaturityModel mapTerms(String[] terms) {
    		PrincipalAtMaturityModel model = new PrincipalAtMaturityModel();
    try{
    		model.setStatusDate(terms[4]);
        model.setContractRole(terms[5]);
        model.setContractID(terms[7]);
        model.setCycleAnchorDateOfInterestPayment(terms[9]);
        model.setCycleOfInterestPayment(terms[10]);
        model.setNominalInterestRate(Double.parseDouble(terms[11]));
        model.setDayCountConvention(terms[12]);
        // accrued interest
        // ipced
        // cey
        // ipcp
        model.setCurrency(terms[16]);
        model.setContractDealDate(terms[17]);
        model.setInitialExchangeDate(terms[18]);
        model.setMaturityDate(terms[19]);
        model.setNotionalPrincipal(Double.parseDouble(terms[20]));
        model.setPurchaseDate(terms[21]);
              model.setPriceAtPurchaseDate(Double.parseDouble(terms[22]));
        model.setTerminationDate(terms[23]);
              model.setPriceAtTerminationDate(Double.parseDouble(terms[24]));
        model.setMarketObjectCodeOfScalingIndex(terms[25]);
        model.setScalingIndexAtStatusDate(Double.parseDouble(terms[26]));
        //model.setCycleAnchorDateOfScalingIndex(terms[28]);
        //model.setCycleOfScalingIndex(terms[29]);
        //model.setScalingEffect(terms[30]);
        model.setCycleAnchorDateOfRateReset(terms[30]);
        model.setCycleOfRateReset(terms[31]);
        model.setRateSpread(Double.parseDouble(terms[32]));
        model.setMarketObjectCodeRateReset(terms[33]);
        model.setCyclePointOfRateReset(terms[34]);
        model.setFixingDays(terms[35]);
        model.setNextResetRate(Double.parseDouble(terms[36]));
        model.setRateMultiplier(Double.parseDouble(terms[37]));
        model.setRateTerm(terms[38]);
      // yield curve correction missing
        model.setPremiumDiscountAtIED(Double.parseDouble(terms[39]));
      
    } catch (Exception e) {
      System.out.println(e.getClass().getName() + 
                         " thrown when mapping terms to ACTUS ContractModel: " +
                        e.getMessage());
    }
    return model;
  }
  
  private static RiskFactorConnector mapRF(Map<String,String[]> rfData, 
                                           String marketObjectCodeRateReset,
                                           String marketObjectCodeScaling,
                                           ZonedDateTime t0) {
    RiskFactorConnector rfCon = new RiskFactorConnector();
    SpotRateCurve curve = new SpotRateCurve();
    SimpleReferenceRate refRate = new SimpleReferenceRate();
    SimpleReferenceIndex refIndex = new SimpleReferenceIndex();
    SimpleForeignExchangeRate fxRate = new SimpleForeignExchangeRate();
    String[] rf;
    String[] keys = rfData.keySet().toArray(new String[rfData.size()]);
    try{
      if(!marketObjectCodeRateReset.equals("NULL")) {
       rf = rfData.get(marketObjectCodeRateReset);
       if(rf[2].equals("TermStructure")) {
         curve.of(t0, PeriodConverter.of(rf[3].split("!")), DoubleConverter.ofArray(rf[4],"!"));
         rfCon.add(rf[0], curve);
       } else if(rf[2].equals("ReferenceRate")){
         refRate.of(DateConverter.of(rf[3].split("!")), DoubleConverter.ofDoubleArray(rf[4],"!"));
         rfCon.add(rf[0], refRate);         
       } 
      }
      if(!marketObjectCodeScaling.equals("NULL")) {
        rf = rfData.get(marketObjectCodeScaling);
         refIndex.of(DateConverter.of(rf[3].split("!")), DoubleConverter.ofDoubleArray(rf[4],"!"));
         rfCon.add(rf[0], refIndex);         
    	}
    } catch(Exception e) {
      System.out.println(e.getClass().getName() + 
                         " when converting risk factor data to actus objects: " +
                        e.getMessage());
    }
    
    return rfCon;
  }
  
        @Override
		public Row call(String s) throws Exception {
          
          // map input file to contract model 
          // (note, s is a single line of the input file)
          PrincipalAtMaturityModel pamModel = mapTerms(s.split(";"));
          
          // map risk factor data to actus connector
//          RiskFactorConnector rfCon = mapRF(riskFactors.value(), 
//                                            pamModel.getMarketObjectCodeRateReset(),
//                                            pamModel.getMarketObjectCodeOfScalingIndex(),
//                                            t0.value());
          RiskFactorConnector rfCon = mapRF(riskFactors, 
                  pamModel.getMarketObjectCodeRateReset(),
                  pamModel.getMarketObjectCodeOfScalingIndex(),
                  t0);
    
          // init actus contract type
          PrincipalAtMaturity pamCT = new PrincipalAtMaturity();
          pamCT.setContractModel(pamModel);
          pamCT.setRiskFactors(rfCon);
          pamCT.generateEvents(t0);
          pamCT.processEvents(t0);
          EventSeries events = pamCT.getProcessedEventSeries();

          //Original:          
//           stucture results als array of Rows
          int nEvents = events.size();
          String[] id = new String[nEvents];
          String[] dt = new String[nEvents];
          String[] cur = new String[nEvents];
          Double[] nv = new Double[nEvents];
          Double[] na = new Double[nEvents];
          StateSpace states;
          for(int i=0;i<nEvents;i++) {
            dt[i] = events.get(i).getEventDate().toString();
            cur[i] = events.get(i).getEventCurrency().toString();
            states = events.get(i).getStates();
            nv[i] = states.getNominalValue();
            na[i] = states.getNominalAccrued();
           
          }
          Arrays.fill(id,0,nEvents,pamModel.getContractID());
          
//          Original:
          Row results = RowFactory.create(id,
                                         dt,
                                         events.getEventTypes(),
                                         cur,
                                         events.getEventValues(),
                                         nv,
                                         na);
          
          //Als Vorschlag hier die Arrays wieder "rückbauen":
          
      	System.out.println("System.out.println(results);");
          System.out.println(results);
                		
          
          // DataFrame vorbereiten
          StructType eventsSchema = DataTypes
                  .createStructType(new StructField[] {
                      DataTypes.createStructField("id", DataTypes.createArrayType(DataTypes.StringType), false),
                      DataTypes.createStructField("date", DataTypes.createArrayType(DataTypes.StringType), false),
                      DataTypes.createStructField("type", DataTypes.createArrayType(DataTypes.StringType), false),
                      DataTypes.createStructField("currency", DataTypes.createArrayType(DataTypes.StringType), false),
                      DataTypes.createStructField("value", DataTypes.createArrayType(DataTypes.DoubleType), false),
                      DataTypes.createStructField("nominal", DataTypes.createArrayType(DataTypes.DoubleType), false),
                      DataTypes.createStructField("accrued", DataTypes.createArrayType(DataTypes.DoubleType), false)});
          
          
            //Dynamische Grösse der Arrays pro Zeile:
            int size =Array.getLength(results.get(0));
            
            //Ausgabefile erstellen
            JavaRDD<Row> Zeilen; 
            List<Row> lines = new ArrayList<>(7);
        	System.out.println("System.out.println(lines);");
            System.out.println(lines);
            
            
            
            //eine Ziele pro Ausgabe
            for (int i=0; i < size ; i++){
            	System.out.println("System.out.println(lines); <- Loop");
            	System.out.println(lines);
            	System.out.println(Array.get((results.get(0)), i ) );
//            lines.add(RowFactory.create(  Array.get((results.get(0)), i ) //"id",
//			              			+";"+ Array.get((results.get(1)), i ) // "date"
//			              			+";"+ Array.get((results.get(2)), i ) // "type"
//			              			+";"+ Array.get((results.get(3)), i ) // "currency"
//			              			+";"+ Array.get((results.get(4)), i ) // "value"
//			              			+";"+ Array.get((results.get(5)), i ) // "nominal"
//			              			+";"+ Array.get((results.get(6)), i ) // "accrued"
//			              			));
            }
            
            //Daten schreiben:
           // Dataset<Row> cachedEvents = sparkSession.createDataFrame(lines, eventsSchema).cache();
    	    //cachedEvents.show();   
         	//cachedEvents.write().parquet(outputPath + "events.parquet");
//        	cachedEvents.write().csv("hdfs://160.85.30.40/user/spark/data/output/ActusPerLine.csv");
            
        //Versuch mit SQL:
            //DataFrame erstellen 
            JavaRDD<Row> rows = sparkSession.emptyDataFrame(). results; 
            results.jav
            //rows.m
            Dataset<Row> cachedTemp = sparkSession.createDataFrame( rows, eventsSchema).cache();
            cachedTemp.createOrReplaceTempView("temp");
            
           // Data Frame durchgehen:
            
            	int sizeSQL =  sparkSession.sql("select size(id) as Size from temp").collectAsList().get(0).getInt(0);
            
            for (int i=0; i < sizeSQL ; i++){
         			sparkSession.sql("insert into eventsSeq select * from temp");
            }
            
            
          
          return results;
        }
      } 
