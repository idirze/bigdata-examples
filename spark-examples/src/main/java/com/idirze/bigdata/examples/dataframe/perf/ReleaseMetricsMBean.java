package com.idirze.bigdata.examples.dataframe.perf;

import java.util.Map;

public interface ReleaseMetricsMBean {
    String getApp();
    void setApp(String app);

    String getVersion();
    void setVersion(String app);

    Map<String, String> getExecutionEngine();
    void setExecutionEngine(Map<String, String> executionEngine);

     Map<String, String> getTags();
     void setTags(Map<String, String> tags);

}