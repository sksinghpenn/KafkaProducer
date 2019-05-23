package com.lifotech.storm.stormKafkaIntegration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;


public class KafkaRandomWordGeneratorConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRandomWordGeneratorConsumer.class);


    public static void main(String[] args) {

        Properties properties = new Properties();

        //set up bootstrap server properties
        properties.setProperty("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

        //setup deserializer
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        //auto commit
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id", "mytestGroupID");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        TopicPartition topicPartition0 = new TopicPartition("stephenTest",0);

        OffsetAndMetadata offsetAndMetadata0 = kafkaConsumer.committed(topicPartition0);

        if (offsetAndMetadata0 != null) {
            logger.info("offsetAndMetadata.offset() for partition 0 " + offsetAndMetadata0.offset());
        }

        TopicPartition topicPartition1 = new TopicPartition("stephenTest",1);

        OffsetAndMetadata offsetAndMetadata1 = kafkaConsumer.committed(topicPartition1);

        if (offsetAndMetadata1 != null) {
            logger.info("offsetAndMetadata.offset() for partition 1 " + offsetAndMetadata1.offset());
        }

        TopicPartition topicPartition2 = new TopicPartition("stephenTest",2);

        OffsetAndMetadata offsetAndMetadata2 = kafkaConsumer.committed(topicPartition2);

        if (offsetAndMetadata2 != null) {
            logger.info("offsetAndMetadata.offset() for partition 2 " + offsetAndMetadata2.offset());
        }


        kafkaConsumer.subscribe(Arrays.asList("stephenTest"));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(2000);

            if (consumerRecords.isEmpty()) {
                try {
                    logger.info("sleeping as did not find any reocrd to consume");
                    Thread.sleep(3000);
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }

            // you can also sleep and wake up and restarting consuming
            for (ConsumerRecord consumerRecord : consumerRecords) {

                logger.info("consumerRecord.key() " + consumerRecord.key());
                logger.info("consumerRecord.value() " + consumerRecord.value());
                logger.info("consumerRecord.offset() " + consumerRecord.offset());
                logger.info("consumerRecord.timestamp() " + consumerRecord.timestamp());
                logger.info("consumerRecord.partition() " + consumerRecord.partition());


            }

        }


        //logger.info("Finished consuming all the record");


    }
}
