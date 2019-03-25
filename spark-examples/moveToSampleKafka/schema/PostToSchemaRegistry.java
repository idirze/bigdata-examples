package com.idirze.bigdata.examples.streaming.moveToSampleKafka.schema;

import com.idirze.bigdata.examples.streaming.moveToSampleKafka.utils.AvroUtils;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.avro.Schema;

import java.io.IOException;

public class PostToSchemaRegistry {

    public static void main(String[] args) throws IOException {


        /**
         * Backward compatibility (default): A new schema is backward compatible if it can be used to read the data written in all previous schemas.
         * Forward compatibility: A new schema is forward compatible if all previous schemas can read data written in this schema.
         * Full compatibility: A new schema is fully compatible if it’s both backward and forward compatible.
         * No compatibility: A new schema can be any schema as long as it’s a valid Avro.
         */
        Schema schema = AvroUtils.loadSchema("src/main/resources/stocks_v1.avsc");

        final OkHttpClient client = new OkHttpClient();
        // Post the schema to the schema registry
        Request request = new Request.Builder()
                .post(RequestBody.create(MediaType
                        .parse("application/vnd.schemaregistry.v1+json"), toRegistryFormat(schema)))
                .url("http://localhost:8081/subjects/"+schema.getName()+"/versions")
                .build();

        String output = client.newCall(request).execute().body().string();
        System.out.println(output);

        request = new Request.Builder()
                .url("http://localhost:8081/subjects")
                .build();
        output = client.newCall(request).execute().body().string();
        System.out.println("Found Schemas:  "+output);


        // Use none: https://github.com/confluentinc/schema-registry/issues/518
        //  Update compatibility requirements globally
        // or use : "type": ["int", "null"] (long => int)
        request = new Request.Builder()
                .put(RequestBody.create(MediaType
                        .parse("application/vnd.schemaregistry.v1+json"), "{\"compatibility\": \"backward\"}"))
                .url("http://localhost:8081/config/"+schema.getName())
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println("Change config: "+output);

        request = new Request.Builder()
                .url("http://localhost:8081/config/"+schema.getName())
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println("Config: "+output);


    }

    private static String toRegistryFormat(Schema schema){
        return "{\"schema\":\""+schema.toString().replace("\"", "\\\"") +"\"}";
    }
}
