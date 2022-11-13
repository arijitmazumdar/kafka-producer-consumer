package com.arijit.kafka.producer_consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Hello world!
 *
 */
public class Consumer {

    static final Logger log = LoggerFactory.getLogger(Consumer.class);
    /**
     * @param args
     */
    public static void main( String[] args )     {
        log.info("I am a Kafka Producer");

        String bootstrapServers = "localhost:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                org.apache.kafka.common.serialization.LongDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, bootstrapServers)
        //properties.setProperty(ConsumerConfig., "500");
        //properties.setProperty(ConsumerConfig.BATCH_SIZE_CONFIG, Integer.toString(64*1024));
        //properties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.arijit.kafka.producer_consumer.InterceptorImpl");
        //properties.setProperty(ConsumerConfig.PARTITIONER_CLASS_CONFIG, bootstrapServers)

        final KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("topic"));
        Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                        //log.info("Producer is flushed and closed");
                        consumer.close();
                }

        });

        while(true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<Long, String> consumerRecord : records) {                
                        log.info("Value:"+consumerRecord.value()+", key:" + consumerRecord.key());
                }
        }
        
    }
}
