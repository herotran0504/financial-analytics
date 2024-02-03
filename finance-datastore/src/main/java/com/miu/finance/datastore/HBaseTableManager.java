package com.miu.finance.datastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTableManager {

    public static void createTableIfNotExisted(String tableName) {
        Configuration hBaseConfig = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(hBaseConfig);
             HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin()) {

            if (!hBaseAdmin.tableExists(tableName)) {
                createHBaseTable(hBaseAdmin, tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createHBaseTable(HBaseAdmin hBaseAdmin, String tableName) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        tableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("cf")));

        hBaseAdmin.createTable(tableDescriptor);
        System.out.println("HBase table created: " + tableName);
    }

}
