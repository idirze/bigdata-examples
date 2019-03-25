package com.idirze.bigdata.examples.streaming.moveToSampleKafka.producer;

import com.idirze.bigdata.examples.streaming.moveToSampleKafka.schema.JavaStockEventDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Consumer {
    public static void main(String[] args) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.24:9092");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("group.id", "stocks_v1_01");
        props.put("auto.offset.reset", "earliest");
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaConsumer consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("stocks_v2"));

        boolean stop = false;
        while (!stop) {
            ConsumerRecords<String, GenericData.Record> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() > 0) {
                //print each record.
                consumerRecords.forEach(record -> {
                    System.out.println("Record Key: " + record.key());
                    System.out.println("Record value: " + record.value());
                    System.out.println("Record partition: " + record.partition());
                    System.out.println("Record offset: " + record.offset());
                });
                // commits the offset of record to broker.
                consumer.commitAsync();
            }
        }

        consumer.close();
    }
}