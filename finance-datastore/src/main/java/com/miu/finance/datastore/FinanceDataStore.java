package com.miu.finance.datastore;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.miu.finance.model.FinanceData;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

public class FinanceDataStore implements DataStore {

    private static final String TABLE_NAME = "finance_historical_data_table";
    private static final String COLUMN_FAMILY = "cf";

    @Override
    public void persist(String jsonRecord) throws Exception {
        try (Connection hbaseConnection = ConnectionFactory.createConnection(HBaseConfiguration.create());
             Table hbaseTable = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME))) {

        	Gson gson = new Gson();
        	FinanceData financeData = gson.fromJson(jsonRecord, FinanceData.class);
        	long date = financeData.getPrices().get(0).getDate();
        	Put put = new Put(Bytes.toBytes(Long.toString(date)));

        	// Convert the YahooFinanceData object to a Map using Gson
        	String jsonProperties = gson.toJson(financeData);
        	Map<String, Object> propertiesMap = gson.fromJson(jsonProperties, new TypeToken<Map<String, Object>>() {}.getType());

        	for (Map.Entry<String, Object> entry : propertiesMap.entrySet()) {
        	    // Convert all values to string for simplicity, you can handle different data types appropriately
        	    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
        	}

        	hbaseTable.put(put);
        }
    }
}
