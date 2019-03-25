package tools.com.idirze.bigdata.examples.writeparquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetFileWrite {

    public static void main(String[] args) {
        // First thing - parse the schema as it will be used
        Schema schema = parseSchema();
        List<GenericData.Record> recordList = getRecords(schema);
        writeToParquet(recordList, schema);
    }

    private static Schema parseSchema() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            // pass path to schema
            schema = parser.parse(ClassLoader.getSystemResourceAsStream(
                    "EmpSchema.avsc"));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;

    }

    private static List<GenericData.Record> getRecords(Schema schema) {
        List<GenericData.Record> recordList = new ArrayList<GenericData.Record>();

        for (int i = 0; i < 1000000; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            // Adding 2 records
            record.put("id", i);
            record.put("Name", "emp"+i);
            record.put("Dept", "D"+i);
            recordList.add(record);
        }

        return recordList;
    }


    private static void writeToParquet(List<GenericData.Record> recordList, Schema schema) {
        // Path to Parquet file in HDFS
        Path path = new Path("data/datalake/lake/perf/parquet/EmpRecord.parquet");
        ParquetWriter<GenericData.Record> writer = null;
        // Creating ParquetWriter using builder
        try {
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build();

            for (GenericData.Record record : recordList) {
                writer.write(record);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}