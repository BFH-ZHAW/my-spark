package com.bruttel.actus;

import java.io.Serializable;


public class EventsBean implements Serializable {
	//Instance Variables
	 private String riskScenario;
	 private String portfolio;
	 private String id;
	 private String date;
	 private String type;
	 private String currency;
	 private double value;
	 private double nominal;
	 private double accrued;
	 private double discount; 
	
	public EventsBean() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the riskScenario
	 */
	public String getRiskScenario() {
		return riskScenario;
	}

	/**
	 * @param riskScenario the riskScenario to set
	 */
	public void setRiskScenario(String riskScenario) {
		this.riskScenario = riskScenario;
	}

	/**
	 * @return the portfolio
	 */
	public String getPortfolio() {
		return portfolio;
	}

	/**
	 * @param portfolio the portfolio to set
	 */
	public void setPortfolio(String portfolio) {
		this.portfolio = portfolio;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the date
	 */
	public String getDate() {
		return date;
	}

	/**
	 * @param date the date to set
	 */
	public void setDate(String date) {
		this.date = date;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the currency
	 */
	public String getCurrency() {
		return currency;
	}

	/**
	 * @param currency the currency to set
	 */
	public void setCurrency(String currency) {
		this.currency = currency;
	}

	/**
	 * @return the value
	 */
	public double getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(double value) {
		this.value = value;
	}

	/**
	 * @return the nominal
	 */
	public double getNominal() {
		return nominal;
	}

	/**
	 * @param nominal the nominal to set
	 */
	public void setNominal(double nominal) {
		this.nominal = nominal;
	}

	/**
	 * @return the accrued
	 */
	public double getAccrued() {
		return accrued;
	}

	/**
	 * @param accrued the accrued to set
	 */
	public void setAccrued(double accrued) {
		this.accrued = accrued;
	}

	/**
	 * @return the discount
	 */
	public double getDiscount() {
		return discount;
	}

	/**
	 * @param discount the discount to set
	 */
	public void setDiscount(double discount) {
		this.discount = discount;
	}

}
