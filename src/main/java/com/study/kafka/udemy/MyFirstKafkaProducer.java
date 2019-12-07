package com.study.kafka.udemy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyFirstKafkaProducer {
    static String bootStrapServers = "127.0.0.1:9092";

    public static void main(String[] args) {

        // Producer properties
        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProducerProperties);

        // Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("my_first_topic", "Hello World");

        // Finally, send data
        producer.send(record);

        // Flush producer
        producer.flush();

        // Flush and close producer
        producer.close();

    }
}
