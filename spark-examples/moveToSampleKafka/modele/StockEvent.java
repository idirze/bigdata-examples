package com.idirze.bigdata.examples.streaming.dstream;

import lombok.Data;

import java.io.Serializable;

@Data
public class StockEvent implements Serializable {

    private long date;
    private double open;
    private double high;
    private double low;
    private double close;
    private double adjClose;
    private long volume;
    private String stock;

    private double sumClose = 0;

    public StockEvent(long date,
                      double open,
                      double high,
                      double low,
                      double close,
                      double adjClose,
                      long volume,
                      String stock) {
        this.date = date;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjClose = adjClose;
        this.volume = volume;
        this.stock = stock;
    }

    public StockEvent sumClose(double sumClose) {
        this.sumClose = this.sumClose + sumClose;
        return this;
    }

}

