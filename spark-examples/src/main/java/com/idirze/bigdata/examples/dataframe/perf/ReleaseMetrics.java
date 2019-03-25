package com.idirze.bigdata.examples.dataframe.perf;


import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ReleaseMetrics implements ReleaseMetricsMBean {
   private String app;
   private String version;
   private Map<String, String> executionEngine = new HashMap<>();
   private Map<String, String> tags = new HashMap<>();

   public ReleaseMetrics(String app, String version) {
      this.app = app;
      this.version=version;
   }

   public ReleaseMetrics withExecutionEngine(String name, String version){
      this.executionEngine.put("name", name);
      this.executionEngine.put("version", version);
      return this;
   }

   public ReleaseMetrics addTag(String name, String value){
      tags.put(name, value);
      return this;
   }

}