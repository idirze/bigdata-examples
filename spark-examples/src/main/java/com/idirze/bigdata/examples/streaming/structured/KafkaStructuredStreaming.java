package com.idirze.bigdata.examples.streaming.structured;

import com.idirze.bigdata.examples.streaming.listener.QueryMetricsListener;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import scala.concurrent.duration.Duration;

import static org.apache.spark.sql.functions.*;

public class KafkaStructuredStreaming {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Stocks - KafkaStructuredStreaming")
                //.config("spark.sql.autoBroadcastJoinThreshold", 1)
                .config("spark.sql.shuffle.partitions", "2")// reduce number of tasks (shuffle): creating too many shuffle partitions:
                .getOrCreate();

        sparkSession
                .streams()
                .addListener(new QueryMetricsListener());

        // 1- Use

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
                .option("checkpointLocation", "data/streaming/structured/kafka_checkpoint")
                .start();

        /**
         * Structured streaming was stable sing Apache Spark 2.2,
         * Perform better due to code generation and the Catalyst optimizer.
         *
         * Kafka Options:
         * - Prefix by: kafka.
         * You cannot set:
         * - group.id: Kafka source will create a unique group id for each query automatically.
         * - auto.offset.reset: Set the source option startingOffsets to specify where to start instead.
         *   => resuming will always pick up from where the query left off
         * - key.deserializer: Keys are always deserialized as byte arrays with ByteArrayDeserializer.
         *   Use DataFrame operations to explicitly deserialize the keys.
         * - value.deserializer: Values are always deserialized as byte arrays with ByteArrayDeserializer.
         *   Use DataFrame operations to explicitly deserialize the values.
         * - key.serializer: Keys are always serialized with ByteArraySerializer or StringSerializer.
         *   Use DataFrame operations to explicitly serialize the keys into either strings or byte arrays.
         * - value.serializer: values are always serialized with ByteArraySerializer or StringSerializer.
         *   Use DataFrame oeprations to explicitly serialize the values into either strings or byte arrays.
         * - enable.auto.commit: Kafka source doesn’t commit any offset.
         * - interceptor.classes: Kafka source always read keys and values as byte arrays.
         *   It’s not safe to use ConsumerInterceptor as it may break the query.
         */

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

        Dataset<Row> streamingAggQuery = stockEvents
                .filter("open > 100 and close > 105")
                .groupBy("stock")
                .count();

        Dataset<Row> streamingTransformationQuery = streaming
                .withColumn("diff_open_close", expr("close - open"))
                .select("stock", "open", "close", "diff_open_close")
                .where(abs(col("diff_open_close")).$greater(5));

        //  Dataset<Row> query= stockEvents.groupBy("stock").count();
        StreamingQuery sq1 = streamingAggQuery
                .writeStream()
                .queryName("streaming_agg_query")
                .format("console")
                .outputMode(OutputMode.Complete())
                // Depending on the sink:
                // same concept as save modes on static DataFrames
                // Append: only add new records to the output sink (default): ensures that each row is output once
                // Update: update changed records in place (only the rows that are different from the previous write are written out to the sink)
                //         - equivalent to append mode when no aggregations
                // Complete: rewrite the full output (Useful when all rows are expected to change over time)
                // .outputMode(OutputMode.Append())
                // => Continuous processing does not support Aggregate operations
                // Triggers the write: Prevent writing many small output files when the sink is a set of files.
                //.trigger(Trigger.Continuous(Duration.apply("30 s")))
                .trigger(Trigger.ProcessingTime(Duration.apply("30 s"))) //U can also trigger the output once: Initial load
                .start();

        StreamingQuery sq2 = streamingTransformationQuery
                .writeStream()
                .queryName("streaming_transformation_query")
                .format("console")
                .outputMode(OutputMode.Append())
                // Depending on the sink:
                // same concept as save modes on static DataFrames
                // Append: only add new records to the output sink (default): ensures that each row is output once
                // Update: update changed records in place (only the rows that are different from the previous write are written out to the sink)
                //         - equivalent to append mode when no aggregations
                // Complete: rewrite the full output (Useful when all rows are expected to change over time)
                // .outputMode(OutputMode.Append())
                // => Continuous processing does not support Aggregate operations
                // Triggers the write: Prevent writing many small output files when the sink is a set of files.
                //.trigger(Trigger.Continuous(Duration.apply("30 s")))
                .trigger(Trigger.ProcessingTime(Duration.apply("30 s"))) //U can also trigger the output once: Initial load
                .start();


        sq1.awaitTermination();
        sq2.awaitTermination();

    }

}
