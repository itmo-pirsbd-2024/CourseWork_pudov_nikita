package ru.highloadjava.coursework.serialization;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.clients.producer.ProducerRecord;
import ru.highloadjava.coursework.datamodel.CryptoAggregatedData;

public class CryptoAggregatedDataSerializationSchema implements KafkaRecordSerializationSchema<CryptoAggregatedData> {

    private static final long serialVersionUID = 1L;
    private final String topic;
    private static final ObjectMapper objectMapper =
            JsonMapper.builder()
                    .build()
                    .registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    // Конструктор, принимающий топик Kafka
    public CryptoAggregatedDataSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            CryptoAggregatedData element, KafkaSinkContext context, Long timestamp) {

        try {
            // Сериализуем ключ (symbol) в байты
            byte[] key = element.getSymbol().getBytes();

            // Сериализуем объект CryptoAggregatedData в JSON
            byte[] value = objectMapper.writeValueAsBytes(element);


            // Создаем и возвращаем ProducerRecord с ключом и значением
            return new ProducerRecord<>(
                    topic,         // Тема Kafka
                    null,           // партиция будет выбрана автоматически
                    element.getEvent_time(),      // временная метка
                    key,            // ключ - это symbol
                    value           // значение - это сериализованный объект CryptoAggregatedData
            );
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + element, e);
        }
    }
}
