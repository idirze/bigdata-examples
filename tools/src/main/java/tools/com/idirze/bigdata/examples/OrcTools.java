
package tools.com.idirze.bigdata.examples;

import org.apache.orc.tools.FileDump;

public class OrcTools {


    public static void main(String[] args) throws Exception {
        execute( "--json",
                "--pretty",
                "--rowindex", // show rowGroupIndexes (strides stats) for column 8
                "6,8",
                "/Users/idir/MyProjects/workspace/bigdata-examples/spark-examples/data/result/orc");
    }

    public static void execute(String... args) throws Exception {
        FileDump.main(args);
    }

}