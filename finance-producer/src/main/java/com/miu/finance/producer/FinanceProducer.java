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

    public static void start() {
        String bootstrapServers = "localhost:9092";
        String topic = "finance_historical_data";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String startDate = "2023-12-12";
        long startTimestamp = FinanceUtils.getTimeStamp(LocalDateTime.parse(startDate + "T00:00:00"));
        LocalDate today = LocalDate.now();
        long endTimestamp = FinanceUtils.getTimeStamp(today);

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            String historicalData = FinanceAPIClient.fetchHistoricalData("AAPL", startTimestamp, endTimestamp);

            FinanceData financeData = gson.fromJson(historicalData, FinanceData.class);

            producer.send(new ProducerRecord<>(topic, "key", gson.toJson(financeData)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

