package org.finance.generator;

import com.miu.finance.model.FinanceData;
import com.miu.finance.model.FinanceDataHBase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.finance.hbasereader.HBaseReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.miu.finance.utils.FinanceUtils.TABLE_NAME;

public class FinanceGenerator {

    public static void main(String[] args) throws IOException {
        start();
    }

    public static void start() throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create()); Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            scan.setCacheBlocks(false);
            scan.setCaching(10000);
            scan.setMaxVersions(10);

            ResultScanner scanner = table.getScanner(scan);
            List<FinanceDataHBase> bases = HBaseReader.getFinanceAnalysis(scanner);
            SparkConf conf = new SparkConf().setAppName("FinanceDataAnalysis").setMaster("local[*]");
            JavaSparkContext sc = null;
            SparkSession spark = null;

            try {
                sc = new JavaSparkContext(conf);
                spark = SparkSession.builder()
                        .appName("FinanceDataAnalysis")
                        .config(conf)
                        .getOrCreate();
                generateFinanceDataAnalysis(sc, spark, bases);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (spark != null) {
                    spark.stop();
                }
                if (sc != null) {
                    sc.close();
                }
            }
        }
    }

    private static void generateFinanceDataAnalysis(JavaSparkContext sc, SparkSession spark, List<FinanceDataHBase> financeDataHBases) {
        JavaRDD<FinanceDataHBase> financeDataRDD = sc.parallelize(financeDataHBases.stream().limit(100).collect(Collectors.toList()));
        String schemaString = "key date open high low close volume adjclose";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, getDataType(fieldName), true);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = financeDataRDD.map((FinanceDataHBase record) -> {
            final FinanceData.Price price = record.getPrice();
            return RowFactory.create(record.getKey(),
                    price.getDate(),
                    price.getOpen(),
                    price.getHigh(),
                    price.getLow(),
                    price.getClose(),
                    price.getVolume(),
                    price.getAdjclose()
            );
        });

        Dataset<Row> financeDataFrame = spark.createDataFrame(rowRDD, schema);
        financeDataFrame.createOrReplaceTempView("financeData");

        Dataset<Row> summaryResult = spark.sql("SELECT * FROM financeData WHERE key != 'NULL'");
        summaryResult.show(5);

        summaryResult.write().mode("append").option("header", "true").csv("hdfs://localhost/user/cloudera/FinanceDataSummary");
    }

    private static DataType getDataType(String fieldName) {
        if (fieldName.equals("key")) return DataTypes.StringType;
        if (fieldName.equals("date") || fieldName.equals("volume")) return DataTypes.LongType;
        return DataTypes.DoubleType;
    }
}

