package com.idirze.bigdata.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.File;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class PrepareData {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[5]")
                .appName("Stocks - PreProcess")
                .config("spark.sql.shuffle.partitions", "50") // used by groupBy to create 100 ordered json files for streaming
                .getOrCreate();
        // Convert csv files into parquet and orc formats, save in the lake zone
        prepareSymbols(sparkSession);
        prepareStocks(sparkSession);

    }


    private static void prepareSymbols(SparkSession sparkSession) {

        // Infer schema
        // These types are supported by spark:
        // https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/types/package-summary.html
        // If you need to read the data from hive, you need to check if that type was supported according to the format (orc, parquet)
        StructType schema = new StructType()
                .add("Company", "string")
                .add("Symbol", "string")
                .add("WeightPercent", "decimal(3,2)");

        Dataset<Row> symbols = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .load("data/datalake/raw/symbols/");


        symbols.printSchema();
        symbols.show();

        // Write as parquet
        symbols
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("data/datalake/lake/symbols/parquet");

        // Write as orc
        symbols
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .orc("data/datalake/lake/symbols/orc");

    }





    private static void prepareStocks(SparkSession sparkSession) {

        sparkSession.udf()
                .register("getStockName", (String fullPath) ->
                        new File(fullPath).getName().replaceAll(".csv", ""), StringType);

        // These types are supported by spark:
        // https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/types/package-summary.html
        // If you need to read the data from hive, you need to check if that type was supported according to the format (orc, parquet)
        // Ex.: date is supported by Hive for orc, but not for parquet (use Hive +1.2)
        StructType schema = new StructType()
                .add("date", "date")
                .add("open", "double")
                .add("high", "double")
                .add("low", "double")
                .add("close", "double")
                .add("adjClose", "decimal(9,5)") // Use Spark +2.4 for predicate push down on decimals
                //.add("adjClose", "double")
                .add("volume", "long");

        Dataset<Row> stocks = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .load("data/datalake/raw/stocks_csv/")
                .withColumn("stock", callUDF("getStockName", input_file_name()));

        stocks.show();
        stocks.printSchema();


        // Write as parquet
        stocks
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("data/datalake/lake/stocks/parquet");

        // Write as orc
        stocks
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                //.option("orc.compress", "snappy") default snappy
                //.option("orc.compress.size", 262144)// default
                //.option("orc.create.index", "false")// does not disable row indexes (strides)
                //.option("orc.row.index.stride", 2500)// default 10 000
                //.option("orc.bloom.filter.columns", "")
                //.option("orc.bloom.filter.fpp", 0.5)
                .orc("data/datalake/lake/stocks/orc");

        // Write 50 json files (globally ordered by date) for streaming
        stocks
                .orderBy("date")
                .write()
                .mode(SaveMode.Overwrite)
                .json("data/streaming/stocks_json");

    }

}
