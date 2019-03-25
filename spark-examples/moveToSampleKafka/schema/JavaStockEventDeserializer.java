package com.idirze.bigdata.examples.streaming.moveToSampleKafka.schema;

import com.idirze.bigdata.examples.streaming.dstream.StockEvent;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

public class JavaStockEventDeserializer implements Deserializer<StockEvent> {
    @Override
    public void configure(Map<String, ?> configs, boolean b) {

    }

    @Override
    public StockEvent deserialize(String topic, byte[] stockEvent) {

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(stockEvent))) {
            Object value = ois.readObject();
            ois.close();

            return value ==null? null: (StockEvent) value;

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() {

    }
}
