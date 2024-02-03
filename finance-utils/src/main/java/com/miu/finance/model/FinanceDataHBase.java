package com.miu.finance.model;

import com.miu.finance.model.FinanceData.Price;

public class FinanceDataHBase {
	FinanceData.Price price; 
	String key;
	public FinanceDataHBase(Price price, String key) {
		super();
		this.price = price;
		this.key = key;
	}
	public FinanceDataHBase() {

		this.price = new FinanceData.Price() ;
		this.key = "";
	}

	public FinanceData.Price getPrice() {
		return price;
	}
	public void setPrice(FinanceData.Price price) {
		this.price = price;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}

}
