package com.miu.finance.producer;

import com.google.gson.Gson;
import com.miu.finance.model.FinanceData;
import com.miu.finance.utils.FinanceUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDate;
import java.util.Properties;

public class FinanceProducer {
    private static final int NUMB_OF_DAYS = 15;
    private static final Gson gson = new Gson();
    private static final String[] SYMBOLS = {"MSFT"};

    public static void start() {
        String bootstrapServers = "localhost:9092";
        String topic = "finance_historical_data";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(NUMB_OF_DAYS);
        long endTime = FinanceUtils.getTimeStamp(endDate);
        long startTime = FinanceUtils.getTimeStamp(startDate);
        for (String key : SYMBOLS) {
            try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
                String historicalData = FinanceAPIClient.fetchHistoricalData(key, startTime, endTime);

                FinanceData financeData = gson.fromJson(historicalData, FinanceData.class);
                for (FinanceData.Price price : financeData.getPrices()) {
                    if (price.getOpen() != 0.0 && price.getVolume() != 0) {
                        producer.send(new ProducerRecord<>(topic, key, gson.toJson(price)));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
