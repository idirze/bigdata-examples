package com.idirze.bigdata.examples.dataframe.agg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;

import static org.apache.spark.sql.functions.*;


public class Aggregation {

    public static void main(String[] args) {

        AggOptions options =  CommandLine.populateCommand(new AggOptions(), args);

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Stocks - Aggregation")
                .getOrCreate();

        Dataset<Row> df = sparkSession
                .read()
                .parquet("data/datalake/lake/stocks/parquet");

        df.printSchema();
        df.show();

        System.out.println("- Compute the average closing price per year for each stock");
        df.select(col("stock"),
                year(col("date")).alias("year"),
                col("adjClose")).alias("avgAdjClose")
                .groupBy("stock", "year")
                .avg("adjClose")
                .orderBy(desc("year"))
                .show();


        System.out.println("- Compute the average closing price per year for Apple");
        df.select(col("stock"),
                year(col("date")).alias("year"),
                col("adjClose")).alias("avgAdjClose")
                .where("stock = 'AAPL'")
                .groupBy("stock", "year")
                .avg("adjClose")
                .orderBy(desc("year"))
                .show();


        System.out.println("- What was the average closing price per month for Walmart");
        df.select(col("stock"),
                year(col("date")).alias("year"),
                month(col("date")).alias("month"),
                col("adjClose")).alias("avgAdjClose")
                .where("stock = 'WMT'")
                .groupBy("stock", "year", "month")
                .avg("adjClose")
                .orderBy(desc("year"), desc("month"))
                .show();

        df
                .groupBy("stock")
                .agg(max("high"), avg("adjClose"))
                .withColumnRenamed("max(high)", "maxHigh")
                .withColumnRenamed("avg(adjClose)", "AvgAdjClose")
                .show();

    }


}
