package com.idirze.bigdata.examples.streaming.moveToSampleKafka.utils;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.google.common.io.Files;
import com.idirze.bigdata.examples.streaming.dstream.StockEvent;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class AvroUtils {

    public static Schema loadSchema(String path) throws IOException {
       return new Schema.Parser()
                .parse(Files.toString(new File(path), Charset.defaultCharset()));

    }

    public static void main(String[] args) throws JsonMappingException {
      System.out.println(  objectToAvroShema(StockEvent.class));
    }

    public static <T> String objectToAvroShema(Class<T> event) throws JsonMappingException {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(event, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();

        org.apache.avro.Schema avroSchema = schemaWrapper.getAvroSchema();
        String asJson = avroSchema.toString(true);

        return asJson;

    }
}
