
package com.idirze.bigdata.examples.tools;

import org.apache.parquet.tools.Main;
public class ParquetTools {


    public static void main(String[] args) {
      //   execute("meta", "--debug", "data/dow-jones-stocks-33RowGroups.parquet");
       // execute("dump", "--disable-data", "--disable-crop", "data/dowJones-stocks.parquet");
        // execute("head", "--debug", "data/dowJones-stocks.parquet");
        // execute("cat", "--debug", "data/dowJones-stocks.parquet");
        //execute("meta", "data/result/stocks/part-00000-2b65e1f8-b52f-4036-b2c7-130f78573176-c000.snappy.parquet");
        execute("meta", "--debug", "/Users/idir/MyProjects/workspace/bigdata-examples/spark-examples/data/datalake/lake/stocks_row_groups/parquet/part-00000-71772eae-d269-4098-97a4-706695eb9603-c000.snappy.parquet");

    }

    public static void execute(String... args) {
        Main.main(args);
    }

}