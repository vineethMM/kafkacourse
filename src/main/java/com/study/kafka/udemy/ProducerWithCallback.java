package com.study.kafka.udemy;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    static String bootStrapServers = "127.0.0.1:9092";

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getName());

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
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null){
                    // Log the error
                    logger.info("******************************************************************************************");
                    logger.info("Received meta data: "+ recordMetadata.topic() + " partition: " + recordMetadata.partition());
                    logger.info("******************************************************************************************");
                } else {
                    logger.error("Error while sending record", e);
                }
            }
        });

        // Flush producer
        producer.flush();

        // Flush and close producer
        producer.close();

    }
}
