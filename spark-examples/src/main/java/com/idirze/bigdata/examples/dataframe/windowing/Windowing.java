package com.idirze.bigdata.examples.dataframe.windowing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.*;

public class Windowing {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Stocks - Windowing")
                .getOrCreate();

        Dataset<Row> stocks = sparkSession
                .read()
                .parquet("data/datalake/lake/stocks/parquet");

        WindowSpec movingAverageWindow20 = Window.orderBy("date").rowsBetween(-20, 0);
        WindowSpec movingAverageWindow50 = Window.orderBy("date").rowsBetween(-50, 0);
        WindowSpec movingAverageWindow100 = Window.orderBy("date").rowsBetween(-100, 0);

        Dataset<Row> stocksMA = stocks
                .withColumn( "MA20", avg(stocks.col("adjClose"))
                        .over(movingAverageWindow20))
                .withColumn( "MA50", avg(stocks.col("adjClose"))
                        .over(movingAverageWindow50))
                .withColumn("MA100", avg(stocks.col("adjClose"))
                        .over(movingAverageWindow100));

        stocksMA.show();

        stocksMA
                .where("close > MA50")
                .select(col("date"), col("close"), col("MA50"))
                .show();

    }

}
