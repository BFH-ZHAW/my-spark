package com.bruttel.actus;


import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.RowFactory;

import scala.Tuple2;

import org.actus.conversion.DateConverter;
import org.actus.conversion.DayCounterConverter;
import org.actus.contracttypes.PrincipalAtMaturity;
import org.actus.models.PrincipalAtMaturityModel;
import org.actus.util.time.EventSeries;
import org.actus.conversion.PeriodConverter;
import org.actus.enumerations.DayCountConventions;
import org.actus.conversion.DoubleConverter;
import org.actus.contractstates.StateSpace;
import org.actus.riskfactors.InterestRateConnector;
import org.actus.riskfactors.RiskFactorConnector;
import org.actus.time.DayCounter;
import org.actus.misc.riskfactormodels.SpotRateCurve;
import org.actus.misc.riskfactormodels.SimpleReferenceRate;
import org.actus.misc.riskfactormodels.SimpleReferenceIndex;
import org.actus.misc.riskfactormodels.SimpleForeignExchangeRate;
import org.actus.financial.conventions.daycount.ActualThreeSixtyFiveFixed;
import org.actus.financial.conventions.daycount.DayCount;
import org.actus.financial.conventions.daycount.DayCountConvention;

import javax.time.calendar.Period;
import javax.time.calendar.ZonedDateTime;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


@SuppressWarnings("serial")
public class ContToEvExtFlatFunc_old implements FlatMapFunction< Tuple2<String[], String[]> ,Row> {
	  ZonedDateTime t0;

  
	  public ContToEvExtFlatFunc_old(ZonedDateTime t0) {
		  this.t0 = t0;

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
    
    //Methode um den RisikoFaktor zu berechnen
    private static RiskFactorConnector mapRF(String[] rfData, 
                                           String marketObjectCodeRateReset,
                                           String marketObjectCodeScaling,
                                           ZonedDateTime t0) {
    RiskFactorConnector rfCon = new RiskFactorConnector();
    SpotRateCurve curve = new SpotRateCurve();
    SimpleReferenceRate refRate = new SimpleReferenceRate();
    SimpleReferenceIndex refIndex = new SimpleReferenceIndex();
    SimpleForeignExchangeRate fxRate = new SimpleForeignExchangeRate();
    String[] rf;
    //String[] keys = rfData.keySet().toArray(new String[rfData.size()]);
    try{
      if(!marketObjectCodeRateReset.equals("NULL")) {
       //rf = rfData.get(marketObjectCodeRateReset);
    	 rf = rfData;
       if(rf[2].equals("TermStructure")) {
         curve.of(t0, PeriodConverter.of(rf[3].split("!")), DoubleConverter.ofArray(rf[4],"!"));
         rfCon.add(rf[0], curve);
       } else if(rf[2].equals("ReferenceRate")){
         refRate.of(DateConverter.of(rf[3].split("!")), DoubleConverter.ofDoubleArray(rf[4],"!"));
         rfCon.add(rf[0], refRate);         
       } 
      }
      if(!marketObjectCodeScaling.equals("NULL")) {
       // rf = rfData.get(marketObjectCodeScaling);
    	  rf = rfData;
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
    
//    - Neues Feild: IdiosyncraticSpread: double type
//    - Vor der schlaufe: RiskFactorConnector erstellen (brauchen wir sowieso für die berechnung der events)
//    - In der schlaufe: für event mit currency XX und date YY, aus RiskFactorConnector zinskurvenobjekt mit schlüssel YC_XX auslesen. 
//    	Aus zinskurvenobject YC_XX dann den zins für datum YY holen mittels „.getRate(AD0, YY)“ -> 
//      diskont faktor rechnen: Math.exp(-YearFraction(AD0,YY)*(zins+IdiosyncraticSpread) => Risiko des Kontrakts (Zusätzlicher Aufschlag) 
    
//    - Dann, diskont faktor als weitere spalte an events array anhängen
    
    @SuppressWarnings("unchecked")
	@Override
		public Iterator<Row> call(Tuple2<String[], String[]> s) throws Exception {
          
          // map input file to contract model 
          // (note, s is a single line of the input file)
          PrincipalAtMaturityModel pamModel = mapTerms(s._1());
                   
          // map risk factor data to actus connector
          RiskFactorConnector rfCon = mapRF(s._2(), 
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
          
          //Wird für den Diskontfaktor gebraucht
          InterestRateConnector<SpotRateCurve> inRate = (InterestRateConnector<SpotRateCurve>) rfCon.get(rfCon.getKeys()[0]);
          DayCounter dc = DayCounterConverter.of(DayCountConventions.ActualActualISDA);
          Double creditSpread = Double.parseDouble(s._1()[41]);
          Double idiosyncraticSpread = Double.parseDouble(s._1()[42]);

          //stucture results als array of Rows
          int nEvents = events.size();
          String[] rs = new String[nEvents]; //Risk Set
          String[] po = new String[nEvents]; //Portfolio
          String[] id = new String[nEvents];
          String[] dt = new String[nEvents];
          String[] cur = new String[nEvents];
          Double[] nv = new Double[nEvents];
          Double[] na = new Double[nEvents];
          Double[] di = new Double[nEvents]; //discountedInterest
          StateSpace states;
          for(int i=0;i<nEvents;i++) {
        	rs[i] = s._2()[1];//RiskSet
        	po[i] = s._1()[44]; //Portfolio 
            dt[i] = events.get(i).getEventDate().toString();
            cur[i] = events.get(i).getEventCurrency().toString();
            states = events.get(i).getStates();
            nv[i] = states.getNominalValue();
            na[i] = states.getNominalAccrued();
                 Double zins = inRate.getRateAt(t0, Period.between(t0, events.get(i).getEventDate()));
            di[i] = Math.exp(-dc.yearFraction(t0, events.get(i).getEventDate())*(zins+creditSpread+idiosyncraticSpread)); //Diskontfaktor    
          }
          Arrays.fill(id,0,nEvents,pamModel.getContractID());
        
          //Row with Arrays
          Row results = RowFactory.create(rs,  	  
        		  						 po,
        		  						 id,
                                         dt,
                                         events.getEventTypes(),
                                         cur,
                                         events.getEventValues(),
                                         nv,
                                         na,
                                         di);
         
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
