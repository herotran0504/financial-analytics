package com.miu.finance.datastore;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.miu.finance.model.FinanceData;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

import static com.miu.finance.utils.FinanceUtils.TABLE_NAME;

public class FinanceDataStore implements DataStore {
    private static final String COLUMN_FAMILY = "cf";
    private static final Gson gson = new Gson();

    public static void start() {
        HBaseTableManager.createTableIfNotExisted(TABLE_NAME);
    }

    @Override
    public void persist(String key, String jsonRecord) throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create()); Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {
            System.out.println("[FinanceDataStore]persist(" + jsonRecord + ')');

            FinanceData.Price price = gson.fromJson(jsonRecord, FinanceData.Price.class);
            long date = price.getDate();
            Put put = new Put(Bytes.toBytes(key + date));

            String jsonProperties = gson.toJson(price);
            Map<String, Object> propertiesMap = gson.fromJson(jsonProperties, new TypeToken<Map<String, Object>>() {
            }.getType());

            for (Map.Entry<String, Object> entry : propertiesMap.entrySet()) {
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
            }

            table.put(put);
        }
    }
}
