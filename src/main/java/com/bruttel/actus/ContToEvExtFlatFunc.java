package com.bruttel.actus;


import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.RowFactory;

import org.actus.conversion.DateConverter;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class ContToEvExtFlatFunc implements FlatMapFunction<String,Row> {
	  ZonedDateTime t0;
	  Map<String,String[]> riskFactors;
  
	  public ContToEvExtFlatFunc(ZonedDateTime t0, Map<String,String[]> riskFactors) {
		  this.t0 = t0;
		  this.riskFactors = riskFactors;
	  }
	   	  
	//Methode um das Maturity Modell zu berechnen  
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
    
    //Methode um den RisikoFacktor zu berechnen
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
  
	//Dieser Aufruf (call String) wird von der FlatMapFunction gebraucht, hier wird festgelegt, wie der Input verarbeitet wird.
    	@Override
		public Iterator<Row> call(String s) throws Exception {
          
          // map input file to contract model 
          // (note, s is a single line of the input file)
          PrincipalAtMaturityModel pamModel = mapTerms(s.split(";"));
          
          // map risk factor data to actus connector
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

          //stucture results als array of Rows
          int nEvents = events.size();
          String[] id = new String[nEvents];
          String[] dt = new String[nEvents];
          String[] cur = new String[nEvents];
          Double[] nv = new Double[nEvents];
          Double[] na = new Double[nEvents];
          Double[] di = new Double[nEvents]; //discountedInterest
          StateSpace states;
          for(int i=0;i<nEvents;i++) {
            dt[i] = events.get(i).getEventDate().toString();
            cur[i] = events.get(i).getEventCurrency().toString();
            states = events.get(i).getStates();
            nv[i] = states.getNominalValue();
            na[i] = states.getNominalAccrued();
            di[i] = states.getInterestCalculationBase(); //Ist das richtig? oder: states.getMaximumDeferredInterest()          
          }
          Arrays.fill(id,0,nEvents,pamModel.getContractID());
        
          //Row with Arrays
          Row results = RowFactory.create(events.get,  	  
        		  						 rp,
        		  						 id,
                                         dt,
                                         events.getEventTypes(),
                                         cur,
                                         events.getEventValues(),
                                         nv,
                                         na);
         
          //Dynamische Grösse der Arrays pro Zeile:
          int size =Array.getLength(results.get(0));
            
          //Ausgabefile erstellen (Ausgabe ist Row, aber RowList ist mehrfaches von Row und somit bei Flatmap erlaubt)
		  List<Row> rowList = new ArrayList<>();
                    
		  //eine Ziele pro Ausgabe
            for (int i=0; i < size ; i++){       	
      		rowList.add(RowFactory.create(  Array.get((results.get(0)), i ), // "riskFactor",
      										Array.get((results.get(1)), i ), // "portfolio",
      										Array.get((results.get(2)), i ), // "id",
			              					Array.get((results.get(3)), i ), // "date"
			              					Array.get((results.get(4)), i ), // "type"
			              					Array.get((results.get(5)), i ), // "currency"
			              					Array.get((results.get(6)), i ), // "value"
			              					Array.get((results.get(7)), i ), // "nominal"
			              					Array.get((results.get(8)), i ), // "accrued"
			              					Array.get((results.get(9)), i )  // "defferedInterest",
			              				));
            }

  		  return rowList.iterator();
        }
      } 
