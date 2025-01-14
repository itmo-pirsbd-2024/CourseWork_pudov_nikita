package ru.highloadjava.coursework;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BinanceKlineMessage {
    @JsonProperty("k")
    private KlineData klineData;

    // Геттеры и сеттеры
    public KlineData getKlineData() {
        return klineData;
    }

    public void setKlineData(KlineData klineData) {
        this.klineData = klineData;
    }
}
