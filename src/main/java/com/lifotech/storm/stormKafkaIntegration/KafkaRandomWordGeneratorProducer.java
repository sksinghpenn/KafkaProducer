package com.lifotech.storm.stormKafkaIntegration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class KafkaRandomWordGeneratorProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRandomWordGeneratorProducer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();

        // add bootstrap server
        properties.setProperty("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

        // add serializer
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // craeate bathc size
        properties.setProperty("batch.size", "500");

        // create linger time
        properties.setProperty("linger.ms", "5");

        //acks
        properties.setProperty("acks", "1");

        // create KafkaProducer
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        String topicName = "";

        long i = 1;
        String[] words = {"he","hi","shashi","ravi","shashi","nitin","allapur","allahabd","jhunsi","lucknow","jhansi"};

        long key = i;
        while (true){
            key =  key + 1;
            String val =  words[new Random().nextInt(11)]+key;
            // create Producer record
            ProducerRecord producerRecord = new ProducerRecord("stephenTest", String.valueOf(key), val);
            // send the records
            kafkaProducer.send(producerRecord);
            logger.info("sent the record " + val);
            try {
                logger.info("sleeping for 1 second  ");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }




    }
}
