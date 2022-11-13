package com.arijit.kafka.producer_consumer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Hello world!
 *
 */
public class Producer {
        private static final class CallbackImplementation implements Callback {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                                log.info("Received new metadata. \n" +                                
                                "Topic:" + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                                
                        } else {
                                if (exception instanceof RetriableException) {
                                        log.error("Retrieable exception");
                                } else {
                                        log.error("Non retriable exception");
                                        
                                }
                                
                        }
                }
        }
        static final Logger log = LoggerFactory.getLogger(Producer.class);
    /**
     * @param args
     */
    public static void main( String[] args )     {
        log.info("I am a Kafka Producer");

        String bootstrapServers = "localhost:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                LongSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "500");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64*1024));
        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.arijit.kafka.producer_consumer.InterceptorImpl");
        //properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, bootstrapServers)

        final KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
                final ProducerRecord<Long, String> producerRecord =
                new ProducerRecord<>("topic", System.currentTimeMillis(), "hello world-"+ i);
        
                producer.send(producerRecord, new CallbackImplementation());
                
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                        //log.info("Producer is flushed and closed");
                        producer.flush();
                        producer.close();
                }

        });
        System.exit(1);
    }
}
