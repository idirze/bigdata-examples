package com.idirze.bigdata.examples.streaming.dstream;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;

public class KafkaDStreaming {

    public static void main(String[] args) throws InterruptedException {

        /**
         * 1- Prefere DirectKafka approach over the Receiver one:
         *   - One to one mapping between rdd and topic partitions (the Receiver approache uses one rdd for all partitions)
         *   - Number of partitions around 2-3 times of number of cores
         * 2- Commit offsets manualy into kafka (commitAsync) instead of relying on chekpointing (saves the state + offsets, upgrades pb, io, etc)
         * 3- Set the number of the concurrent jobs:
         *    - By default, spark triggers one job (batch) at a time
         *    -  spark.streaming.concurrentJobs
         * 4- Handle unbalanced partitions:
         *    - Some partitions may be large than others
         *    - Increases computation time, cpu remains idles (waiting)
         *    - spark.streaming.kafka.maxRatePerPartition = batch interval * nb messages
         *    => Sometimes it might happen that some kafka topics/partitions send data at very high rate while some send at very low.
         * 5- spark.streaming.unpersist was et by default
         *    INFO BlockManager: Removing RDD 497
         * 6- Enable backpressure in production to handle volume burst
         *    spark.streaming.backpressure.enabled
         * 7- The parameter spark.streaming.blockInterval is used in the receiver approach
         * 8- Disable data locality (suitable massive batch processing):
         *    - spark.locality.wait
         * 9- Maximum number of consecutive retries the driver will make in order to find the latest offsets:
         *    - spark.streaming.kafka.maxRetries: 1 (2 attempts)
         * 10- spark.streaming.ui.retainedBatches (spark ui nb batches)
         * 11- spark.streaming.stopGracefullyOnShutdown
         */
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Stocks - Aggregation")
                // By default the number of concurrent jobs is 1
                .set("spark.streaming.concurrentJobs", "5") // Message ordering not possible, also if nb partitions > 1
                .set("spark.streaming.kafka.maxRatePerPartition", "1")
                .set("spark.streaming.ui.retainedBatches", "10")
                .set("spark.locality.wait", "0"); // 4 ;

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.24:9092");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stocks_v2_0_8");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        // Batch Interval: Create new batch every 10 seconds
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Duration.apply(10));

        JavaInputDStream<ConsumerRecord<String, GenericData.Record>> stocksEvent = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(asList("stocks_v2"), props));

        JavaDStream<StockEvent> stocks = stocksEvent
                .map(e -> convert(e.value()));

        //foreachRDD: executed on the driver, the rdd transformations inside will be executed on the execs
        // returns void
        stocks.foreachRDD(rdd -> {
           // OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreach(event -> System.out.println("Stock: " + event + ", with offset ranges: "));
        });


        // Operate on every element of the rdd
        JavaDStream<Double> ddl =stocks.map(x -> x.getHigh());
        // rdd.join(spamInfoRDD).filter(...); // join data stream with spam information to do data cleaning
        // RDD-to-RDD function to every RDD of the DStream
        JavaDStream<StockEvent> dd = stocks
                .transform(cr -> cr.rdd().toJavaRDD());

        // Windowing
        // reduceByKeyAndWindow ... on pair RDD
        // http://spark.apache.org/docs/latest/streaming-programming-guide.html#transform-operation
        stocks
                .reduceByWindow((x, y) -> x.sumClose(y.getClose()), Duration.apply(30000), Duration.apply(-1000))
        .print();

        // Convert to PairRdd
       JavaPairDStream<String, StockEvent> stocksPair = stocks.mapToPair(x -> new Tuple2<>(x.getStock(), x));
       // Expose partitioner
       // stocksPair.reduceByKeyAndWindow()

        jssc.start();
        jssc.awaitTermination();

    }

    private static StockEvent convert(GenericData.Record record) {
        return new StockEvent(
                Long.valueOf(record.get("date").toString()),
                Double.valueOf(record.get("open").toString()),
                Double.valueOf(record.get("high").toString()),
                Double.valueOf(record.get("low").toString()),
                Double.valueOf(record.get("close").toString()),
                Double.valueOf(record.get("adjClose").toString()),
                Long.valueOf(record.get("volume").toString()),
                record.get("stock").toString());
    }


}
