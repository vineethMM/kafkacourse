package com.study.kafka.udemy;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MessagesWithKeys {
    static String bootStrapServers = "127.0.0.1:9092";

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(MessagesWithKeys.class.getName());

        // Producer properties
        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProducerProperties);

        String topic =  "my_first_topic";

        for(int i = 0; i <= 10; i ++){
            String value = "Hello world: " + Integer.toString(i);
            String key =  "Key: "+ Integer.toString(i);
            // Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,value);

            // Finally, send data
            // The callback allows as to perform some action when the message is send.
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        // Log the error
                        logger.info("\n******************************************************************************************");
                        logger.info("Received meta data: ");
                        logger.info("Topic: "+ recordMetadata.topic());
                        logger.info("Key: " + recordMetadata.);
                        logger.info("Partition: " + recordMetadata.partition());
                        logger.info("Offset: " + recordMetadata.offset());
                        logger.info("******************************************************************************************\n");
                    } else {
                        logger.error("Error while sending record", e);
                    }
                }
            });
        }

        // Flush producer
        // This is required as the `send` method is asynchronous, it forces to flush the messages
        producer.flush();

        // Flush and close producer
        producer.close();

        /*
         * With keys, Kafka guarantees that all the records with same key goes to the same partition, thus
         * guarantees ordering of records/messages within key.
         *
         * Note: as long as, there is no change in partition number
         */
    }
}
