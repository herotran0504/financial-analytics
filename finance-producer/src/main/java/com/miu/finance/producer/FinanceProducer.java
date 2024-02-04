package com.miu.finance.producer;

import com.google.gson.Gson;
import com.miu.finance.model.FinanceData;
import com.miu.finance.utils.FinanceUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;

public class FinanceProducer {

    private static final Gson gson = new Gson();
    private static String[] symbolArray = {
    	"MSFT"
    };

    public static void start() {
        String bootstrapServers = "localhost:9092";
        String topic = "finance_historical_data";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String startDate = "2024-01-01";
        long startTimestamp = FinanceUtils.getTimeStamp(LocalDateTime.parse(startDate + "T00:00:00"));
        LocalDate today = LocalDate.now();
        long endTimestamp = FinanceUtils.getTimeStamp(today);
        for(String key: symbolArray) {

	        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
	            String historicalData = FinanceAPIClient.fetchHistoricalData(key, startTimestamp, endTimestamp);
	
	            FinanceData financeData = gson.fromJson(historicalData, FinanceData.class);
	            for (FinanceData.Price price : financeData.getPrices()) {
	                producer.send(new ProducerRecord<>(topic, key, gson.toJson(price)));
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
        }
    }

}

