package ru.highloadjava.coursework.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import ru.highloadjava.coursework.datamodel.CryptoConsumerData;

import java.io.IOException;

public class CryptoDataDeserializer implements Deserializer<CryptoConsumerData> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CryptoConsumerData deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, CryptoConsumerData.class);
        } catch (IOException e) {
            e.printStackTrace();  // Handle the error
            return null;  // Return null or handle error as needed
        }
    }
}
