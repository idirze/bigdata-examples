package com.idirze.bigdata.examples.streaming.moveToSampleKafka.schema;

import com.idirze.bigdata.examples.streaming.dstream.StockEvent;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class JavaStockEventSerializer implements Serializer<StockEvent> {
    @Override
    public void configure(Map<String, ?> configs, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, StockEvent stockEvent) {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
            oos.writeObject(stockEvent);
            return stream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() {

    }
}
