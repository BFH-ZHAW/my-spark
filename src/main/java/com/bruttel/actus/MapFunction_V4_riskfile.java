package com.bruttel.actus;

import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.RowFactory;

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
//import org.actus.financial.conventions.daycount.ActualThreeSixtyFiveFixed;
//import org.actus.financial.conventions.daycount.DayCount;
//import org.actus.financial.conventions.daycount.DayCountConvention;

import javax.time.calendar.Period;
import javax.time.calendar.ZonedDateTime;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("serial")
public class MapFunction_V4_riskfile implements FlatMapFunction<Row, Row> {
	ZonedDateTime t0;
	List<Row> risks;
	// The method flatMap(Function1<Row,TraversableOnce<U>>, Encoder<U>) in the
	// type Dataset<Row> is not applicable for the arguments (ContToEventFunc)

	public MapFunction_V4_riskfile(List<Row> risks, ZonedDateTime t0) {
		this.t0 = t0;
		this.risks = risks;

	}

	// Methode um das Maturity Modell zu berechnen
	private static PrincipalAtMaturityModel mapTerms(Row terms) {
		PrincipalAtMaturityModel model = new PrincipalAtMaturityModel();
		try {
			model.setStatusDate(terms.getString(4));
			model.setContractRole(terms.getString(5));
			model.setContractID(terms.getString(7));
			model.setCycleAnchorDateOfInterestPayment(terms.getString(9));
			model.setCycleOfInterestPayment(terms.getString(10));
			model.setNominalInterestRate(Double.parseDouble(terms.getString(11)));
			model.setDayCountConvention(terms.getString(12));
			// accrued interest
			// ipced
			// cey
			// ipcp
			model.setCurrency(terms.getString(16));
			model.setContractDealDate(terms.getString(17));
			model.setInitialExchangeDate(terms.getString(18));
			model.setMaturityDate(terms.getString(19));
			model.setNotionalPrincipal(Double.parseDouble(terms.getString(20)));
			model.setPurchaseDate(terms.getString(21));
			model.setPriceAtPurchaseDate(Double.parseDouble(terms.getString(22)));
			model.setTerminationDate(terms.getString(23));
			model.setPriceAtTerminationDate(Double.parseDouble(terms.getString(24)));
			model.setMarketObjectCodeOfScalingIndex(terms.getString(25));
			model.setScalingIndexAtStatusDate(Double.parseDouble(terms.getString(26)));
			// model.setCycleAnchorDateOfScalingIndex(terms[28]);
			// model.setCycleOfScalingIndex(terms[29]);
			// model.setScalingEffect(terms[30]);
			model.setCycleAnchorDateOfRateReset(terms.getString(30));
			model.setCycleOfRateReset(terms.getString(31));
			model.setRateSpread(Double.parseDouble(terms.getString(32)));
			model.setMarketObjectCodeRateReset(terms.getString(33));
			model.setCyclePointOfRateReset(terms.getString(34));
			model.setFixingDays(terms.getString(35));
			model.setNextResetRate(Double.parseDouble(terms.getString(36)));
			model.setRateMultiplier(Double.parseDouble(terms.getString(37)));
			model.setRateTerm(terms.getString(38));
			// yield curve correction missing
			model.setPremiumDiscountAtIED(Double.parseDouble(terms.getString(39)));

		} catch (Exception e) {
			System.out.println(
					e.getClass().getName() + " thrown when mapping terms to ACTUS ContractModel: " + e.getMessage());
		}
		return model;
	}

	// Methode um den RisikoFaktor zu berechnen
	private static RiskFactorConnector mapRF(Row rfData, String marketObjectCodeRateReset,
			String marketObjectCodeScaling, ZonedDateTime t0) {
		RiskFactorConnector rfCon = new RiskFactorConnector();
		SpotRateCurve curve = new SpotRateCurve();
		SimpleReferenceRate refRate = new SimpleReferenceRate();
		SimpleReferenceIndex refIndex = new SimpleReferenceIndex();
		SimpleForeignExchangeRate fxRate = new SimpleForeignExchangeRate();
		// String[] rf;
		// String[] keys = rfData.keySet().toArray(new String[rfData.size()]);
		try {
			if (!marketObjectCodeRateReset.equals("NULL")) {
				// rf = rfData.get(marketObjectCodeRateReset);
				// rf = rfData;
				if (rfData.getString(2).equals("TermStructure")) {
					curve.of(t0, PeriodConverter.of(rfData.getString(3).split("!")),
							DoubleConverter.ofArray(rfData.getString(4), "!"));
					rfCon.add(rfData.getString(0), curve);
				} else if (rfData.getString(2).equals("ReferenceRate")) {
					refRate.of(DateConverter.of(rfData.getString(3).split("!")),
							DoubleConverter.ofDoubleArray(rfData.getString(4), "!"));
					rfCon.add(rfData.getString(0), refRate);
				}
			}
			if (!marketObjectCodeScaling.equals("NULL")) {
				// rf = rfData.get(marketObjectCodeScaling);
				// rf = rfData;
				refIndex.of(DateConverter.of(rfData.getString(3).split("!")),
						DoubleConverter.ofDoubleArray(rfData.getString(4), "!"));
				rfCon.add(rfData.getString(0), refIndex);
			}
		} catch (Exception e) {
			System.out.println(
					e.getClass().getName() + " when converting risk factor data to actus objects: " + e.getMessage());
		}

		return rfCon;
	}

	// Dieser Aufruf (call String) wird von der FlatMapFunction gebraucht, hier
	// wird festgelegt, wie der Input verarbeitet wird.

	// - Neues Feild: IdiosyncraticSpread: double type
	// - Vor der schlaufe: RiskFactorConnector erstellen (brauchen wir sowieso
	// für die berechnung der events)
	// - In der schlaufe: für event mit currency XX und date YY, aus
	// RiskFactorConnector zinskurvenobjekt mit schlüssel YC_XX auslesen.
	// Aus zinskurvenobject YC_XX dann den zins für datum YY holen mittels
	// „.getRate(AD0, YY)“ ->
	// diskont faktor rechnen:
	// Math.exp(-YearFraction(AD0,YY)*(zins+IdiosyncraticSpread) => Risiko des
	// Kontrakts (Zusätzlicher Aufschlag)

	// - Dann, diskont faktor als weitere spalte an events array anhängen

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Row> call(Row r) throws Exception {
		// ... add some names to the collection

		// Ausgabefile erstellen (Ausgabe ist Row, aber RowList ist mehrfaches
		// von Row und somit bei Flatmap erlaubt)
		List<Row> rowList = new ArrayList<>();

		risks.forEach(ris -> {

			// map input file to contract model
			// (note, s is a single line of the input file)
			PrincipalAtMaturityModel pamModel = mapTerms(r);

			// map risk factor data to actus connector
			RiskFactorConnector rfCon = mapRF(ris, pamModel.getMarketObjectCodeRateReset(),
					pamModel.getMarketObjectCodeOfScalingIndex(), t0);

			// init actus contract type
			PrincipalAtMaturity pamCT = new PrincipalAtMaturity();
			pamCT.setContractModel(pamModel);
			pamCT.setRiskFactors(rfCon);
			pamCT.generateEvents(t0);
			pamCT.processEvents(t0);
			EventSeries events = pamCT.getProcessedEventSeries();

			// Wird pro Event gebraucht:
			// Für den Diskont Faktor
			InterestRateConnector<SpotRateCurve> inRate = (InterestRateConnector<SpotRateCurve>) rfCon
					.get(rfCon.getKeys()[0]);
			DayCounter dc = DayCounterConverter.of(DayCountConventions.ActualActualISDA);
			String riskSet = ris.getString(1);
			String portfolio = r.getString(44);
			String id = pamModel.getContractID();
			Double creditSpread = Double.parseDouble(r.getString(41));
			Double idiosyncraticSpread = Double.parseDouble(r.getString(42));

			// Dynamische Grösse der Events:
			int size = events.size();

			// eine Ziele pro Event
			for (int i = 0; i < size; i++) {
				StateSpace states = events.get(i).getStates();
				Double zins = inRate.getRateAt(t0, Period.between(t0, events.get(i).getEventDate()));
				rowList.add(RowFactory.create(riskSet, // "riskSet",
						portfolio, // "portfolio",
						id, // "id",
						events.get(i).getEventDate().toString(), // "date"
						events.get(i).getEventType(), // "type"
						events.get(i).getEventCurrency().toString(), // "currency"
						events.get(i).getEventValue(), // "value"
						states.getNominalValue(), // "nominal"
						states.getNominalAccrued(), // "accrued"
						Math.exp(-dc.yearFraction(t0, events.get(i).getEventDate())
								* (zins + creditSpread + idiosyncraticSpread)) // "defferedInterest",
				));
			}

			// rowList.add(RowFactory.create( risks.get(0).getString(1), //
			// "riskSet",
			// "Portofolio", //r.getString(44), // "portfolio",
			// "ID", //r.getString(7), // "id",
			// "DATE",//pamModel.getMarketObjectCodeRateReset(), //
			// events.get(i).getEventDate().toString(), // "date"
			// "", //events.get(i).getEventType(), // "type"
			// "CHF", //events.get(i).getEventCurrency().toString(), //
			// "currency"
			// Double.parseDouble("100"), //events.get(i).getEventValue(), //
			// "value"
			// Double.parseDouble("200") , //states.getNominalValue(), //
			// "nominal"
			// Double.parseDouble("300"), //r.getString(42)), // "accrued"
			// Double.parseDouble("400") //r.getString(41)) //
			// "defferedInterest",
			// ));

		});

		return rowList.iterator();
	}
}
