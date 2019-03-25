package com.idirze.bigdata.examples.streaming.moveToSampleKafka.producer;

import com.idirze.bigdata.examples.streaming.moveToSampleKafka.utils.AvroUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Properties;

import static com.idirze.bigdata.examples.streaming.moveToSampleKafka.utils.Utils.nexDouble;
import static com.idirze.bigdata.examples.streaming.moveToSampleKafka.utils.Utils.sleep;

@Slf4j
public class Producer {

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.24:9092");
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaProducer kafkaProducer = new KafkaProducer<String, GenericData.Record>(properties);


        // It's a good practice to store the used avro schame alongside the code
        // You can get the schema from the registry, you need to know the version
        // If the used schema is not compatabile or registred in the schema registry:
        // => Schema being registered is incompatible with an earlier schema; error code: 409
        Schema schema = AvroUtils.loadSchema("src/main/resources/stocks_v1.avsc");

        for (int i = 0; i < 1000; i++) {

            GenericData.Record record= new GenericRecordBuilder(schema)
                    .set("date", DateTime.now().toDate().getTime())
                    .set("open", nexDouble())
                    .set("high", nexDouble())
                    .set("low", nexDouble())
                    .set("close", nexDouble())
                    .set("adjClose", nexDouble())
                    .set("volume", 12)
                    .set("stock", "IBM")
            .build();

            kafkaProducer.send(new ProducerRecord("stocks_v2", record));

            sleep(1000);
        }

    }


}
