package com.idirze.bigdata.examples.dataframe.partitioning;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.desc;

public class Partitioning {

    public static void main(String[] args) throws InterruptedException {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Stocks - Partitioning")
                .config("spark.sql.shuffle.partitions", 12)
                .getOrCreate();

        Dataset<Row> df = sparkSession
                .read()
                .parquet("data/datalake/lake/stocks/parquet")
                .select(col("date"),
                        year(col("date")).alias("year"),
                        col("stock"),
                        col("adjClose"),
                        col("open"),
                        col("close"),
                        col("high"),
                        col("volume"));

        //Partition By Year
        df
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("year")
                .parquet("data/datalake/lake/stocks_partionned/parquet");

        // Load the partitioned dataset
        Dataset<Row> stocks = sparkSession
                .read()
                .parquet("data/datalake/lake/stocks_partionned/parquet");

        /**
         * filter and where are equivalent
         */
        Dataset<Row> partitionned = stocks
                //.filter("year = '2010' and stock = 'WMT'")
                //Case 1: where before the select
                // Partition prunning and ppd happen
                //  PartitionFilters: [isnotnull(year#40), (year#40 = 2010)], PushedFilters: [IsNotNull(stock), EqualTo(stock,WMT)],
               // .where("year = '2010' and stock = 'WMT'")
                .select(col("stock"),
                        year(col("date")).alias("year"),
                        month(col("date")).alias("month"),
                        col("adjClose")).alias("avgAdjClose")
                // Case 2: The where is after the select
                // Partition prunning is not happening
                // PartitionFilters: [], PushedFilters: [IsNotNull(stock), EqualTo(stock,WMT)],
                //.where("year = '2010' and stock = 'WMT'")
                .groupBy("stock", "year", "month")
                .avg("adjClose")
                .withColumnRenamed("avg(adjClose)", "AvgAdjClose")
                .orderBy(desc("year"), desc("month"));

        partitionned.explain(true);

        System.out.print(partitionned.count());

        // Go to http://localhost:4040/SQL

        //Thread.sleep(100000000);

    }


}
