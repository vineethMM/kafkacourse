package com.study.kafka.udemy.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    private String bootstrapServer = "127.0.0.1:9092";
    private String topic = "my_first_topic";
    private String consumerGroup = "my-second-application";


    private Properties createConsumerProperties(){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return  properties;
    }

    public KafkaConsumer<String, String> createConsumerAndSubscribe(Properties properties, String topic){
        // Create a consumer with given properties
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe to the given topic
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    private void consumeRecods(){
        KafkaConsumer<String, String> consumer = createConsumerAndSubscribe(createConsumerProperties(), topic);

        while (true){
            ConsumerRecords<String, String>  records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record: records){
                logger.info("\n*********************** Record received ***************************");
                logger.info("Topic: "+ record.topic() );
                logger.info("Partition: "+ record.partition() );
                logger.info("Offset: "+ record.offset() );
                logger.info("Key: "+ record.key() );
                logger.info("Value: "+ record.value() );
                logger.info("*******************************************************************\n");
            }
        }
    }


    public static void main(String[] args) {

        ConsumerDemo consumerDemo = new ConsumerDemo();
        consumerDemo.consumeRecods();
    }

    /**
     * Each consumer in the consumer group read from exclusive partitions of the topic.
     * When a consumer is added/removed the remaining consumers load balances itself.
     * This is really interesting feature.
     *
     * The read offset of topic is committed to `__consumer_offset` topic in in kafka
     * It is a system reserved topic in Kafka.
     *
     * Its important to note that `AUTO_OFFSET_RESET` property is applied at the consumer group level
     * not at the consumer level.
     */

}
