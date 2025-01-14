package ru.highloadjava.coursework;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import ru.highloadjava.coursework.datamodel.CryptoAggregatedData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Service
@Slf4j
public class KafkaConsumerService {

    //private static final String TOPIC = "crypto_data_aggregated"; // Укажите вашу тему Kafka
    //@Getter
    //@Autowired
    private final SimpMessagingTemplate messagingTemplate;

    //@Autowired
    private ConsumerFactory<String, CryptoAggregatedData> kafkaConsumerFactory;
    private final int CACHE_SIZE = 1000; // Максимальный размер кэша

    //@Getter
    private final List<CryptoAggregatedData> dataCache = new ArrayList<>();


    public KafkaConsumerService(SimpMessagingTemplate messagingTemplate, ConsumerFactory<String, CryptoAggregatedData> kafkaConsumerFactory, ObjectMapper objectMapper) {
        this.messagingTemplate = messagingTemplate;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
    }

    /**
     * Возвращает неизменяемое представление кэша данных.
     */
    public List<CryptoAggregatedData> getDataCache() {
        return Collections.unmodifiableList(dataCache);
    }


    /**
     * Kafka Listener для потребления сообщений из Kafka.
     */
    @KafkaListener(topics = "crypto_data_aggregated")
    public void handleKafkaMessage(@Payload CryptoAggregatedData message) {
        log.info("Получено сообщение из Kafka: {}", message);
        System.out.println("Consumed data: {" + message + "}");

        if (dataCache.size() >= CACHE_SIZE) {
            dataCache.remove(0); // Удаляем самое старое сообщение
        }
        // Добавляем в локальный кэш
        dataCache.add(message);

        // Сортируем кэш по event_time
        dataCache.sort(Comparator.comparing(CryptoAggregatedData::getEvent_time));

        // Отправляем сообщение всем подписанным WebSocket клиентам
        this.messagingTemplate.convertAndSend("/topic/updates", message);
    }
}
