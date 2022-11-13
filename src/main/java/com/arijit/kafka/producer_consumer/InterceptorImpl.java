package com.arijit.kafka.producer_consumer;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterceptorImpl implements ProducerInterceptor {
    static final Logger log = LoggerFactory.getLogger(InterceptorImpl.class);
    public  InterceptorImpl() {
    }
        @Override
        public void configure(Map<String, ?> configs) {
                // TODO Auto-generated method stub
                log.info(configs.toString());
               return; 
        }

        @Override
        public ProducerRecord onSend(ProducerRecord record) {
                // TODO Auto-generated method stub
                log.info("Before Send -->" + record.value());
                record.headers().add("custom-header", "some value".getBytes());
                return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
                // TODO Auto-generated method stub
                log.info("On acknowledgement -->" + metadata);
        }

        @Override
        public void close() {
                // TODO Auto-generated method stub
                
        }

}