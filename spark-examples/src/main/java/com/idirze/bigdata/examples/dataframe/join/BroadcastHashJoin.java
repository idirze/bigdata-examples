package com.idirze.bigdata.examples.dataframe.join;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BroadcastHashJoin {

    public static void main(String[] args) throws InterruptedException {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Stocks - SQL")
                //.config("spark.sql.autoBroadcastJoinThreshold", 1)
                .getOrCreate();

        Dataset<Row> stocks = sparkSession
                .read()
                .parquet("data/datalake/lake/stocks/parquet");

        Dataset<Row> symbols = sparkSession
                .read()
                .parquet("data/datalake/lake/symbols/parquet");


        // Spark chooses between two join implementations:
        // 1- broadcast hash join:
        //    - Broadcast the smaller table into all executors to prevent shuffling
        //      1- Hash the join keys of the smaller table, sort them, and store the corresponding rows in the hash table entries.
        //      2- Join the Bigger and the smaller table rows if the key from the bigger one match the key of the hash table
        //      => The keys of the bigger table are not sorted
        //    - Controlled by the parameters:
        //      1- spark.sql.autoBroadcastJoinThreshold (default 10 MB max, if <=10MB broadcast)
        //      2- spark.sql.broadcastTimeout
        // 2- sort merge join:
        //    1- Sort the join keys on both tables
        //    2- Perform an interleaved scan (When matching join keys are encountered, the rows from both tables can be merged, thus executing the join.)
        //    => Expensive as the bigger table need to be sorted so shuffled
        //    => Data can be spilled to disk
        // => shuffle hash-join: disabled by default (The smaller table needs to fit in memory)
        // => If broadcast hash join is not applicable, fallback to sort merge join (enumarete cases where not applicable)


        /**
         *     --  Test broadcast hash join --
         *     Spark UI SQL:  Optimized query (the filter is executed after the stocks scan)
         */
        stocks.join(symbols, stocks.col("stock").equalTo(symbols.col("symbol")), "inner")
                //.where("adjClose > 500")
                .where(stocks.col("adjClose").$greater(300))
                .select("date", "Company", "adjClose", "volume")
                .count();
         // Perf equivalent to (optimized plan)
        /*stocks
                .filter(stocks.col("adjClose").$greater(300))
                .join(symbols, stocks.col("stock").equalTo(symbols.col("symbol")), "inner")
                .select("date", "Company", "adjClose", "volume")
                .count();*/

        //Spark UI SQL:
        // .count();
        // .show();
        //.explain(true);

        /** Go to http://localhost:4040/SQL
         To see the physical plan visualization for the executed query
         1- There are two stages, one for each table (stocks + symbols).
         2- The scan operator scan the columns: date,adjClose,volume, stock from the table socks
            Scan the columns: Company, symbol from the table symbols
         3- Filter the output with the predicate : (isnotnull(adjClose#5) && (adjClose#5 > 300.0)) && isnotnull(stock#7)
            Filter the output with the predicate: isnotnull(symbol#17
         4- Project the ouput with columns : date#0, adjClose#5, volume#6L, stock#7
            Project the output with columns: Company#16, Symbol#17
         5- Broadcast the hash table (symbols) to the other stage using the BroadcastExchange operator
            => The hashtable is built in with BroadcastExchange operator
            => See the physical plan:  +- BroadcastExchange HashedRelationBroadcastMode(List(input[1, string, true]))
         6- The BroadcastHashJoin operator perform the hash join
         7- Finally, the Project operator outputs the requested columns
        */

        Thread.sleep(100000000);

    }

}
