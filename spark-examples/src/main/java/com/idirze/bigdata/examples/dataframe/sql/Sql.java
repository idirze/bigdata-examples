package com.idirze.bigdata.examples.dataframe.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Sql {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Stocks - SQL")
                .getOrCreate();

        Dataset<Row> df = sparkSession
                .read()
                .parquet("data/datalake/lake/stocks/parquet");


        df.show();
        df.registerTempTable("stocks");

        // Calculate the average closing price per year
        System.out.println("- Calculate the average closing price per month");
        sparkSession
                .sql("select stock, year(date) as year, avg(adjClose) avgAdjClose from stocks group by stock, year order by year desc")
                .show();

        // Calculate the average closing price per month
        System.out.println("- Calculate the average closing price per month");
        sparkSession
                .sql("select stock, year(date) as year, month(date) month, avg(adjClose) avgAdjClose from stocks group by stock, year, month order by year, month desc")
                .show();

        // When did the closing price go up or down by more than 2 dollars
        System.out.println("-  When did the closing price go up or down by more than 2 dollars");
        sparkSession
                .sql("select stock, date, open, close, round((close-open), 6) diff from stocks where abs(close-open)> 2 order by date desc")
                .show();


    }

}
