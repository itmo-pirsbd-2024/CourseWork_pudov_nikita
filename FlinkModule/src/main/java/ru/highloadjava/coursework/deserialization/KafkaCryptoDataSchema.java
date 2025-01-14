package ru.highloadjava.coursework.deserialization;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import ru.highloadjava.coursework.datamodel.CryptoData;

import java.io.IOException;

public class KafkaCryptoDataSchema extends AbstractDeserializationSchema<CryptoData> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    /**
     По соображениям производительности лучше создавать в ObjectMapper в этом открытом методе, а не
     создавать новый ObjectMapper для каждой записи.
     */
    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    /**
     Если бы нашему методу десериализации требовался доступ к информации, содержащейся в заголовках Kafka записи пользователя
     мы бы реализовали Kafka Record Deserialization Schema вместо
     расширения AbstractSchemaDeserialization.
     */
    @Override
    public CryptoData deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, CryptoData.class);
    }
}
