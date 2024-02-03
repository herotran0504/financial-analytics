package org.finance.generator;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.finance.hbasereader.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.AnalysisException;

import com.miu.finance.model.FinanceData;
import com.miu.finance.model.FinanceDataHBase;



public class FinanceGenerator {

	//	public static void start() throws AnalysisException, IOException {
	//
	//		SparkConf conf= new SparkConf().setAppName("FinanceDataAnalysis").setMaster("local[*]");
	//		JavaSparkContext sc=new JavaSparkContext(conf);
	//		SparkSession spark = SparkSession
	//				.builder()
	//				.appName("FinanceDataAnalysis")
	//				.config(conf)
	//				.getOrCreate();
	//
	//		showFinanceDataAnalysis(sc,spark);
	//		spark.stop();
	//		sc.close();
	//	}
//	public static void start() {
//		SparkConf conf = new SparkConf().setAppName("FinanceDataAnalysis").setMaster("local[*]");
//		JavaSparkContext sc = null;
//		SparkSession spark = null;
//		try {
//			sc = new JavaSparkContext(conf);
//			spark = SparkSession.builder()
//					.appName("FinanceDataAnalysis")
//					.config(conf)
//					.getOrCreate();
//
//			showFinanceDataAnalysis(sc, spark);
//		} catch (Exception e) {
//			e.printStackTrace(); // Log the exception
//		} finally {
//			if (spark != null) {
//				spark.stop();
//			}
//			if (sc != null) {
//				sc.close();
//			}
//		}
//	}
//	public static void main() {
	public static void main(String[] args){
		System.out.println("0000000000");
		SparkConf conf = new SparkConf().setAppName("FinanceDataAnalysis").setMaster("local[*]");
		JavaSparkContext sc = null;
		SparkSession spark = null;

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

		StructType schema = DataTypes.createStructType(fields);

		JavaRDD<Row> rowRDD = financeDataRDD.map((FinanceDataHBase record) -> RowFactory.create(record.getKey(), record.getPrice().getDate(), record.getPrice().getOpen(), 
																record.getPrice().getHigh(), record.getPrice().getLow(), record.getPrice().getClose(), record.getPrice().getVolume(), record.getPrice().getAdjclose()));

		Dataset<Row> financeDataFrame = spark.createDataFrame(rowRDD, schema);
		financeDataFrame.createOrReplaceTempView("financeData");

		// Example SQL queries on finance data
		Dataset<Row> summaryResult = spark.sql("SELECT key, date, open, high, low, close, volume FROM financeData WHERE key != 'NULL'");
		summaryResult.show(5);

//		Dataset<Row> avgVolume = spark.sql("SELECT AVG(volume) as avg_volume FROM financeData");
//		avgVolume.show(5);

		// You can modify the file paths according to your HDFS setup or requirements
		summaryResult.write().mode("append").option("header", "true").csv("hdfs://localhost/user/cloudera/FinanceDataSummary");
//		avgVolume.write().mode("append").option("header", "true").csv("hdfs://localhost/user/cloudera/FinanceDataAvgVolume");
	}
}

