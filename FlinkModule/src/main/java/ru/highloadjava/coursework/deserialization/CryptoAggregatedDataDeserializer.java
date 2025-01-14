package ru.highloadjava.coursework.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import ru.highloadjava.coursework.datamodel.CryptoAggregatedData;


import java.io.IOException;

@Slf4j
public class CryptoAggregatedDataDeserializer implements Deserializer<CryptoAggregatedData> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CryptoAggregatedData deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, CryptoAggregatedData.class);
        } catch (IOException e) {
            //e.printStackTrace();  // Handle the error
            log.error("Failed to deserialize data: {}", data, e); // Логируем ошибку и данные
            throw new RuntimeException("Deserialization failed", e); // Прокидываем исключение выше
            //return null;  // Return null or handle error as needed
        }
    }
}
