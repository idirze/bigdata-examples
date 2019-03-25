package com.idirze.bigdata.examples.dataframe.perf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MergeSortJoin {

    public static void main(String[] args) throws Exception {

        SimpleAgent.main();

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Stocks - SQL")
                .config("spark.sql.autoBroadcastJoinThreshold", 1)
                .getOrCreate();

        sparkSession
                .sparkContext()
                .addFile("/Users/idir/MyProjects/workspace/bigdata-examples/spark-examples/src/main/resources/metrics.properties");

        sparkSession
                .sparkContext()
                .addFile("/Users/idir/MyProjects/workspace/jvm-profiler/target/jvm-profiler-1.0.0.jar");


        sparkSession
                .conf()
                .set("spark.metrics.conf", "file:///Users/idir/MyProjects/workspace/bigdata-examples/spark-examples/src/main/resources/metrics.properties");


        sparkSession
                .conf()
                .set("spark.jars", "file:///Users/idir/MyProjects/workspace/jvm-profiler/target/jvm-profiler-1.0.0.jar");


        sparkSession
                .conf()
                .set("spark.driver.extraJavaOptions", "-javaagent:file:///Users/idir/MyProjects/workspace/jvm-profiler/target/jvm-profiler-1.0.0.jar=reporter=com.uber.profiling.reporters.InfluxDBOutputReporter,metricInterval=5000,sampleInterval=5000,ioProfiling=true");

        sparkSession
                .conf()
                .set("spark.executor.extraJavaOptions", "-javaagent:file:///Users/idir/MyProjects/workspace/jvm-profiler/target/jvm-profiler-1.0.0.jar");

        Dataset<Row> df1 = sparkSession
                .read()
                .parquet("data/datalake/lake/perf/EmpRecord-1.parquet").cache();

        Dataset<Row> df2 = sparkSession
                .read()
                .parquet("data/datalake/lake/perf/EmpRecord-2.parquet").cache();


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
        //    => Expensive as the bigger table need to be sorted, so shuffled
        //    => Data can be spilled to disk
        // => shuffle hash-join: disabled by default (The smaller table needs to fit in memory)
        // => If broadcast hash join is not applicable, fallback to sort merge join

        /**
         *     --  Test Merge Sort join --
         */
        int cpt = 1;
        while (true) {
            long count = df1.join(df2, "id")
                    .count();

            if (count != 1000000) {
                System.out.println("Exit with count: " + count + ", cpt: " + (cpt));
                System.exit(0);
            }

            if (cpt++ % 1000 == 0) {
                System.out.println("Progress: " + cpt);
            }
            //       break;
        }

        // .count();
        // .show();
        //.explain(true);

        /** Go to http://localhost:4040/SQL
         Different physical plan for the same query
         With the broadcast hash join, the exec plan consist of 2 wholeStageCode Gen and no shuffle( Exchange Operator)
         With the Merge Sort join, the exec plan created 5 wholeStage code gen, and shuffle each table because a sort
         */

        // Thread.sleep(100000000);

    }

}
