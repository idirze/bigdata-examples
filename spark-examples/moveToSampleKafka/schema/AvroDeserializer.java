package com.idirze.bigdata.examples.streaming.moveToSampleKafka.schema;

import com.databricks.spark.avro.SchemaConverters;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

public class AvroDeserializer extends AbstractKafkaAvroDeserializer {


    private String schemaRegistryUrl = "http://localhost:8081";


    public AvroDeserializer (){}

    public AvroDeserializer(SchemaRegistryClient schemaRegistryClient) {
        super.schemaRegistry = schemaRegistryClient;
    }

    public String deserialize(byte[] payload) {
        GenericRecord genericRecord = (GenericRecord) super.deserialize(payload);
        return genericRecord.toString();
    }

    public AvroDeserializer kafkaAvroDeserializer() {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128);
        AvroDeserializer kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient);

        String avroSchema = null;
        try {
            avroSchema = schemaRegistryClient.getLatestSchemaMetadata("StockEvent").getSchema();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
        SchemaConverters.SchemaType sparkSchema = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema));

        return kafkaAvroDeserializer;
    }


}
