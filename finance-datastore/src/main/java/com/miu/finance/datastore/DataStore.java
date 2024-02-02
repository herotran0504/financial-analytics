package com.miu.finance.datastore;

public interface DataStore {
    static DataStore get() {
        return new FinanceDataStore();
    }

    void persist(String jsonRecord) throws Exception;
}