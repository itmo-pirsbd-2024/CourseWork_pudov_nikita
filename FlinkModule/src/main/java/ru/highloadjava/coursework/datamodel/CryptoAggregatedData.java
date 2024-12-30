package ru.highloadjava.coursework.datamodel;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class CryptoAggregatedData {
    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("average_price")
    private Double average_price;

    @JsonProperty("event_time")
    private Long event_time;

    // Геттеры и сеттеры
    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public Double getAverage_price() {
        return average_price;
    }

    public void setAverage_price(Double average_price) {
        this.average_price = average_price;
    }

    public Long getEvent_time() {
        return event_time;
    }

    public void setEvent_time(Long event_time) {
        this.event_time = event_time;
    }
}
