package ru.highloadjava.coursework.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import ru.highloadjava.coursework.datamodel.CryptoConsumerData;

import java.io.IOException;

@Slf4j
public class CryptoDataDeserializer implements Deserializer<CryptoConsumerData> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CryptoConsumerData deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, CryptoConsumerData.class);
        } catch (IOException e) {
            log.error("Failed to process record from topic: {}", topic, e);
            throw new RuntimeException("Failed to deserialize data from topic: " + topic, e);
            //e.printStackTrace();  // Handle the error
            //return null;  // Return null or handle error as needed
        }
    }
}
