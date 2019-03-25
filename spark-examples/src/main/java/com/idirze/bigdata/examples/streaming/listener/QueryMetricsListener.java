package com.idirze.bigdata.examples.streaming.listener;

import org.apache.spark.sql.streaming.StreamingQueryListener;


/**
 * Push your queries metrics to kafka
 */
public class QueryMetricsListener extends StreamingQueryListener {

    @Override
    public void onQueryStarted(StreamingQueryListener.QueryStartedEvent event) {
        System.out.println("Query Started: " + event.id() + " : "+event);
    }

    @Override
    public void onQueryProgress(StreamingQueryListener.QueryProgressEvent event) {
        System.out.println("Query In progress: " + event.progress().id() +" : "+ event.progress());
    }

    @Override
    public void onQueryTerminated(StreamingQueryListener.QueryTerminatedEvent event) {
        System.out.println("Query terminated: " + event.id() + " : "+ event);
    }

}
