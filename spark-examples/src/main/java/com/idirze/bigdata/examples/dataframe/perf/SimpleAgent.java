package com.idirze.bigdata.examples.dataframe.perf;

import javax.management.*;
import java.lang.management.*;

public class SimpleAgent {
   private MBeanServer mbs = null;

   public SimpleAgent() {

      // Get the platform MBeanServer
       mbs = ManagementFactory.getPlatformMBeanServer();

      // Unique identification of MBeans
      ReleaseMetrics infos = new ReleaseMetrics("cmtOil", "1.4.3");
      infos.withExecutionEngine("Spark", "2.3.0");
      infos.addTag("git.commitId", "29687f094cfc22999527cdc013813774519b7468");
      infos.addTag("release.date", "Sat Dec 15 14:45:09 2018 +0530");
      infos.addTag("other.tag1", "other.value1");

      ObjectName objectName = null;

      try {
         // Uniquely identify the MBeans and register them with the platform MBeanServer 
         objectName = new ObjectName("metrics:app-info=app-release-info");
         mbs.registerMBean(infos, objectName);
      } catch(Exception e) {
         e.printStackTrace();
      }
   }


   public static void main(String ... argv) {
      SimpleAgent agent = new SimpleAgent();
      System.out.println("SimpleAgent is running...");
   }
}