package ru.highloadjava.coursework.datamodel;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CryptoData {
    @JsonProperty("symbol")
    private String symbol;

    /*@JsonProperty("event_time")
    private String event_time;*/

    @JsonProperty("event_time")
    private Long event_time;

    @JsonProperty("close")
    private Double close;

    // Геттеры и сеттеры
    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public Long getEvent_time() {
        return event_time;
    }

    public void setEvent_time(Long event_time) {
        this.event_time = event_time;
    }

    public Double getClose() {
        return close;
    }

    public void setClose(double close) {
        this.close = close;
    }
}
