package com.idirze.bigdata.examples.streaming.moveToSampleKafka.utils;

import org.apache.commons.math3.random.RandomDataGenerator;

public class Utils {

    public static double nexDouble() {
        return new RandomDataGenerator().nextUniform(1.0, 500.0);
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
