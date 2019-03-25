package com.idirze.bigdata.examples.dataframe.ppd;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class PredicatePushDown {

    public static void main(String[] args) throws InterruptedException {

        // spark.sql.codegen.wholeStage enabled by default
        // spark.sql.parquet.enableVectorizedReader
        // Others: org.apache.spark.sql.internal.SQLConf
        //         parquet.block.size=134217728 # in bytes, default = 128 * 1024 * 1024
        // parquet.page.size=1048576 # in bytes, default = 1 * 1024 * 1024
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[5]")
                .appName("Stocks - PredicatePushDown")
                .getOrCreate();

        /**
         * The predicate pushdown happens at a RowGroup level.
         * Since, in our original parquet file we have a single rowgroup (default 12MB), we have splitted horizontally
         * the file into 33 row groups to demonstrate the ppd and row group skipping
         */
        Dataset<Row> df = sparkSession
                .read()
                .parquet("data/datalake/lake/stocks/parquet");

        // We will split the file horizontally into 33 row groups (indexes)
        df
                .coalesce(1)
                .write()
                .option("parquet.block.size", 100 * 1024)  // rowGroup Size
                .mode(SaveMode.Overwrite)
                .parquet("data/datalake/lake/stocks_row_groups/parquet");

        Dataset<Row> stocks = sparkSession
                .read()
                .parquet("data/datalake/lake/stocks_row_groups/parquet");

        // System.out.println("Total: "+df.count()); // 60390
        // From Spark 2.4 on ward, support for pushdown for decimal types and others:
        // https://issues.apache.org/jira/browse/SPARK-25419
        // https://github.com/apache/spark/blob/branch-2.4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFilters.scala
        /**
         *
         * row group 4:  RC:1847: [min: AAPL, max: CVX
         * row group 5:  RC:1853: [min: AAPL, max: MMM
         * The scanner scanned 2 row groups, so the total number scanned was 1847 + 1853 = 3700 shown in Spark UI
         * The filter outputed 2 013 rows
         *
         * If your read data/dow-jones-stocks.parquet (single row group), the scanner will output 60390 (scan the row group)
         * Spark does not push down into coulumn chuncks or pages (Ineficient, Spec V2)
         */
        ppdOnString(stocks);

        /**
         * If the keys are randomly distributed among row groups, there will be a full scan and the ppd will be ineficcient
         * Because, the key will match the min/max in each group and each group will be scanned
         */
        //ppdOnNumber(stocks);
        //ppdOnDecimal(stocks);

        Thread.sleep(100000000);

    }


    /**
     * http://localhost:4040/SQL
     * Test Predicate pushdown on numbers
     * - The predicate adjClose > 200 was actually pushed down:
     * Number of totoal rows: 60390
     * Number of total rows adjClose > 200: 2500
     * <p>
     * The total number of rows scanned by the scan Operator is 2500
     */
    private static void ppdOnDecimal(Dataset<Row> df) {

        long count = df
                .select("stock", "open", "close", "adjClose")
                .where("adjClose > 200.0")
                .count();

        System.out.println("ppdOnDecimal Count: " + count);

    }

    /**
     * http://localhost:4040/SQL
     * Test Predicate pushdown on numbers
     * - The predicate adjClose > 200 was actually pushed down:
     * Number of totoal rows: 60390
     * Number of total rows adjClose > 200: 2500
     * <p>
     * The total number of rows scanned by the scan Operator is 2500
     */
    private static void ppdOnNumber(Dataset<Row> df) {

        long count = df
                .select("stock", "open", "close", "adjClose")
                .where("adjClose > 200")
                .count();

        System.out.println("ppdOnNumber Count: " + count);
    }

    /**
     * http://localhost:4040/SQL
     * Test Predicate pushdown on strings
     * - The predicate stock = 'AAPL' was actually pushed down:
     * Number of totoal rows: 60390
     * Number of total rows for AAPL: 2100
     * <p>
     * The total number of rows scanned by the scan Operator is 2100
     */
    private static void ppdOnString(Dataset<Row> df) {

        long count = df
                .select("stock", "open", "close", "adjClose")
                .where("stock = 'AAPL'")
                .count();

        System.out.println("ppdOnString Count: " + count);

        // Spark execution plans:
        // 1- First the query is parsed (Parsed Logical Plan)
        // 2- The query is converted into a logical plan (Analyzed Logical Plan)
        // 3- The Analyzed Logical Plan is fed into query optimizer yielding into an Optimized Logical Plan
        // 4- The Optimized Logical Plan is converted into a Physical plan
        // Logical Plan vs Physical Plan:
        // - The Logical plan describes WHAT data operations are needed to perform the computation
        // - The Physical Plan describes HOW the plan should be executed on the cluster

        // == Analyzed Logical Plan
        // Filter (stock#7 = AAPL)
        // Note: Spark tranlate the where clause on filter: stock = AAPL

        // == Optimized Logical Plan ==
        // Filter (isnotnull(stock#7) && (stock#7 = AAPL))
        // Note: The filter was then optimized into a bloom filter isnotnull and the original filter stock#7 = AAPL

        // == Physical Plan ==
        // PushedFilters: [IsNotNull(stock), EqualTo(stock,AAPL)]
        // Note: Spark pushes down the filters into the scan (FileScan) parquet operator and evaluating them there
        // it is really only effective when there is a natural ordering to the values (otherwise full scan)
        // - Read the plan from Bottom to Top:
        //   * We have 3 operators: FileScan, Filter and Project operators
        //   * The Project operator has 4 references the columns it projects (stock#7, open#1, close#4, adjClose#5),
        //     a number was added to prevent column name clash
        //   * IsNotNull operator was added as an optimization (metadata operation)
        //   * Note the presence of PartitionFilters (partition pruning) and PushedFilters (row group skipping) in the FileScan operator


        // The three operators are inside of a WholeStageCodegen (single optimizd function) stage and that we have vectorized execution that processed
        // data per batch (a set of column vectors).
        // If the data is randomly distributed, the min/max skippiping will be inefective (full scan, the min/max exists in each page stats but the value could be not present)

    }

}
