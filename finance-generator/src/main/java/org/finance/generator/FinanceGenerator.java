package org.finance.generator;

import com.miu.finance.model.FinanceData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
<<<<<<< Updated upstream
import org.apache.spark.sql.AnalysisException;

import com.miu.finance.model.FinanceData;
import com.miu.finance.model.FinanceDataHBase;

=======
import org.finance.hbasereader.HBaseReader;
>>>>>>> Stashed changes

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FinanceGenerator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("FinanceDataAnalysis").setMaster("local[*]");
        JavaSparkContext sc = null;
        SparkSession spark = null;

<<<<<<< Updated upstream
		try {
			sc = new JavaSparkContext(conf);
			spark = SparkSession.builder()
					.appName("FinanceDataAnalysis")
					.config(conf)
					.getOrCreate();
			System.out.println("66666");
			showFinanceDataAnalysis(sc, spark);
		} catch (Exception e) {
			e.printStackTrace(); // Log the exception
		} finally {
			if (spark != null) {
				spark.stop();
			}
			if (sc != null) {
				sc.close();
			}
		}
	}
	private static void showFinanceDataAnalysis(JavaSparkContext sc, SparkSession spark) throws IOException {
		JavaRDD<FinanceDataHBase> financeDataRDD = sc.parallelize(new HBaseReader().GetFiannceAnalysis());
		// Define the schema based on FinanceData.Price fields
		String schemaString = "key date open high low close volume adjclose";
		
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, fieldName.equals("date") || fieldName.equals("volume") ? DataTypes.LongType : DataTypes.DoubleType, true);
			fields.add(field);
		}
=======
        try {
            sc = new JavaSparkContext(conf);
            spark = SparkSession.builder()
                    .appName("FinanceDataAnalysis")
                    .config(conf)
                    .getOrCreate();
            showFinanceDataAnalysis(sc, spark);
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

    private static void showFinanceDataAnalysis(JavaSparkContext sc, SparkSession spark) throws IOException {
        JavaRDD<FinanceData.Price> financeDataRDD = sc.parallelize(new HBaseReader().getFinanceAnalysis());
        String schemaString = "date open high low close volume adjclose";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, fieldName.equals("date") || fieldName.equals("volume") ? DataTypes.LongType : DataTypes.DoubleType, true);
            fields.add(field);
        }
>>>>>>> Stashed changes

        StructType schema = DataTypes.createStructType(fields);

<<<<<<< Updated upstream
		JavaRDD<Row> rowRDD = financeDataRDD.map((FinanceDataHBase record) -> RowFactory.create(record.getKey(), record.getPrice().getDate(), record.getPrice().getOpen(), 
																record.getPrice().getHigh(), record.getPrice().getLow(), record.getPrice().getClose(), record.getPrice().getVolume(), record.getPrice().getAdjclose()));
=======
        JavaRDD<Row> rowRDD = financeDataRDD.map((FinanceData.Price record) -> RowFactory.create(record.getDate(), record.getOpen(), record.getHigh(), record.getLow(), record.getClose(), record.getVolume(), record.getAdjclose()));
>>>>>>> Stashed changes

        Dataset<Row> financeDataFrame = spark.createDataFrame(rowRDD, schema);
        financeDataFrame.createOrReplaceTempView("financeData");

<<<<<<< Updated upstream
		// Example SQL queries on finance data
		Dataset<Row> summaryResult = spark.sql("SELECT key, date, open, high, low, close, volume FROM financeData WHERE key != 'NULL'");
		summaryResult.show(5);

//		Dataset<Row> avgVolume = spark.sql("SELECT AVG(volume) as avg_volume FROM financeData");
//		avgVolume.show(5);

		// You can modify the file paths according to your HDFS setup or requirements
		summaryResult.write().mode("append").option("header", "true").csv("hdfs://localhost/user/cloudera/FinanceDataSummary");
//		avgVolume.write().mode("append").option("header", "true").csv("hdfs://localhost/user/cloudera/FinanceDataAvgVolume");
	}
=======
        Dataset<Row> summaryResult = spark.sql("SELECT date, open, high, low, close, volume FROM financeData");
        summaryResult.show();

        Dataset<Row> avgVolume = spark.sql("SELECT AVG(volume) as avg_volume FROM financeData");
        avgVolume.show();

        summaryResult.write().mode("append").option("header", "true").csv("hdfs://localhost/user/cloudera/FinanceDataSummary");
        avgVolume.write().mode("append").option("header", "true").csv("hdfs://localhost/user/cloudera/FinanceDataAvgVolume");
    }
>>>>>>> Stashed changes
}

