package com.miu.finance.model;

import java.io.Serializable;

public class FinanceDataHBase implements Serializable {
    String key;
    FinanceData.Price price;

    public FinanceDataHBase() {
        this.key = "";
        this.price = new FinanceData.Price();
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
