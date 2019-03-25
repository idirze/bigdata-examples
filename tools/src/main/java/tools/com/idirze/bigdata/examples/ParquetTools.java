
package tools.com.idirze.bigdata.examples;

import org.apache.parquet.tools.Main;

public class ParquetTools {


    public static void main(String[] args) {
         execute("meta", "--debug", "/Users/idir/MyProjects/workspace/bigdata-examples/spark-examples/data/zones/lake/stocks_row_groups/parquet/part-00000-3626952d-73ee-4e32-baab-81bec1dff1d8-c000.snappy.parquet");
       // execute("dump", "--disable-data", "--disable-crop", "data/dowJones-stocks.parquet");
        // execute("head", "--debug", "data/dowJones-stocks.parquet");
        // execute("cat", "--debug", "data/dowJones-stocks.parquet");
        //execute("meta", "data/result/stocks/part-00000-2b65e1f8-b52f-4036-b2c7-130f78573176-c000.snappy.parquet");
    }

    public static void execute(String... args) {
        Main.main(args);
    }

}