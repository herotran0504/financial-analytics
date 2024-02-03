package com.miu.finance.consumer;

import com.miu.finance.datastore.DataStore;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FinanceConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "finance_historical_consumer_group";
    private static final String TOPIC = "finance_historical_data";

    public static void start() {
        KafkaConsumer<String, String> kafkaConsumer = setupConsumer();
        DataStore dataStore = DataStore.get();

        try {
            processMessages(kafkaConsumer, dataStore);
        } finally {
            closeResources(kafkaConsumer);
        }
    }

    private static KafkaConsumer<String, String> setupConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(properties);
    }

    private static void processMessages(Consumer<String, String> kafkaConsumer, DataStore dataStore) {
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            records.forEach(record -> {
                String jsonRecord = record.value();
                System.out.println("[FinanceConsumer]jsonRecord = " + jsonRecord);
                try {
                    dataStore.persist(jsonRecord);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static void closeResources(Consumer<String, String> kafkaConsumer) {
        try {
            kafkaConsumer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
