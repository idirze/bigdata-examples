package com.idirze.bigdata.examples.streaming.continuous;

import org.apache.kafka.common.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class KafkaContinuousStreamingEventTime {

    /**
     * Use checkpointing for recovery
     * . option("checkpointLocation", "/some/location/")
     */
    public static void main(String[] args) throws StreamingQueryException {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Stocks - FileStructuredStreaming")
                //.config("spark.sql.autoBroadcastJoinThreshold", 1)
                .config("spark.sql.shuffle.partitions", "2")// reduce number of tasks (shuffle): creating too many shuffle partitions:
                .getOrCreate();


        /*sparkSession
                .streams()
                .addListener(new QueryMetricsListener());*/

        // Read the events from json files and push to Kafka
        StructType schema = sparkSession
                .read()
                .json("data/streaming/stocks_json")
                .schema();

        // You need to infer the schema
        Dataset<Row> streaming = sparkSession
                .readStream()
                .schema(schema)
                .option("maxFilesPerTrigger", 1)
                .json("data/streaming/stocks_json");

        // Make the key as null to distribute the data equally among partitions
        streaming
                .selectExpr("null", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.0.24:9092")
                .option("topic", "stocks_structured_v1")
                .option("checkpointLocation", "data/streaming/continuous/kafka_checkpoint")
                .start();

        StructType schemaJson = new StructType()
                .add("date", "date")
                .add("open", "double")
                .add("high", "double")
                .add("low", "double")
                .add("close", "double")
                .add("adjClose", "decimal(9,5)") // Use Spark +2.4 for predicate push down on decimals
                //.add("adjClose", "double")
                .add("volume", "long")
                .add("stock", "string");

        Dataset<Row> stockEvents = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.0.24:9092")
                .option("failOnDataLoss", "false")//true
                .option("fetchOffset.numRetries", "3")// default 3
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", "100") // none, Rate limit on maximum number of offsets processed per trigger interval.
                .option("kafkaConsumer.pollTimeoutMs", "1000")
                .option("subscribe", "stocks_structured_v1")
                .load()
                .selectExpr("cast (value as string) as json")
                .select(from_json(col("json"), schemaJson).as("data"))
                .select("data.*");


        // Creation_Time is the event time in epoch milliseconds
        // Convert it to timestamp

        Dataset<Row> withEventTime = stockEvents
                .selectExpr("*", "cast(open as timestamp) as event_time");

        withEventTime.printSchema();

        // Count the number of event for each 10 minutes
        // Tumbling Window
        StreamingQuery withEventTimeQuery = withEventTime
                // 0 duration by default (:00, :30, :40, ...and not :55 ...)
                .groupBy(window(col("event_time"), "10 minutes"))
                .count()
                .writeStream()
                .queryName("tumbling_events_per_window")
                .outputMode("complete")
                .format("memory")
                .start();

        withEventTime
                // 0 duration by default (:00, :30, :40, ...and not :55 ...)
                .groupBy(window(col("event_time"), "10 minutes"), col("stock"))
                .count()
                .writeStream()
                .queryName("stock_events_per_window")
                .format("memory")
                .outputMode("complete")
                .start();

        // Sliding window
        // window duration: historical data of 10 minutes
        // slideDuration: refresh every 5 minutes (start a window every 5 minutes)
        // Equivalent to have 2 window
        withEventTime.groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
                .count()
                .writeStream()
                .queryName("sliding_events_per_window")
                .format("memory")
                .outputMode("complete")
                .start();


        // Watermark
        // If the watermark not specified, spark need to store that intermediate data forever
        // we expect to see every event within 15 minutes.
        // the engine will keep updating counts of a window in the Result Table until the window is older than the watermark,
        // which lags behind the current event time in column “timestamp” by 10 minutes.
        // Starts by the current watermark (replay) and set the watermark to latest one
        StreamingQuery watermarkStreamingQuery = withEventTime
                .withWatermark("event_time", "15 minutes")
                .groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
                .count()
                .writeStream()
                .queryName("watermark_events_per_window")
                .outputMode("update")
                .format("memory")
                .start();


        // Exactly Once semantics:
        // Structured Streaming makes it easy to take message systems that provide at-least-once semantics,
        // and convert them into exactly-once by dropping duplicate messages as they come in, based on arbitrary keys.
        // Ensure the watermark was enabled to prevent the aggregated dataset from growing (long running jobs)

        StreamingQuery deduplicateQuery = withEventTime
                .withWatermark("event_time", "5 seconds")
                .dropDuplicates("stock", "event_time")
                .groupBy("stock")
                .count()
                .writeStream()
                .queryName("deduplicate_query")
                .format("memory")
                .outputMode("complete")
                .start();


        for (int i = 0; i < 100; i++) {
           /*  System.out.println("-------- tumbling_events_per_window ---------------");
            sparkSession
                    .sql("SELECT * FROM tumbling_events_per_window")
                    .show();

           System.out.println("-------- sliding_events_per_window ---------------");
            sparkSession
                    .sql("SELECT * FROM sliding_events_per_window")
                    .show();*/

            //  "watermark" : "2015-02-24T15:06:45.931Z"
            /*System.out.println("The last progress: " + watermarkStreamingQuery.lastProgress());

            System.out.println("-------- watermark_events_per_window ---------------");
            sparkSession
                    .sql("SELECT * FROM watermark_events_per_window")
                    .show();*/

            sparkSession
                    .sql("SELECT * FROM deduplicate_query")
                    .show();

            Utils.sleep(100000);
        }

        withEventTimeQuery.awaitTermination();
        watermarkStreamingQuery.awaitTermination();
        deduplicateQuery.awaitTermination();
    }

}
