package ru.highloadjava.coursework;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.Date;

import org.json.JSONObject;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WebSocketClientCaller {

    public static void main(String[] args) throws Exception {
        // Инициализация Redis
        RedisClient redisClient = RedisClient.create("redis://localhost:6379");
        RedisCommands<String, String> redisCommands = redisClient.connect().sync();

        // Инициализация Kafka
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // Создаем WebSocket подключение
        WebSocket ws = HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(URI.create("wss://stream.binance.com:9443/ws/btcusdt@kline_1s"),
                        new BinanceWebSocketListener(redisCommands, kafkaProducer))
                .join();

        // Поддерживаем приложение активным
        while (true) {
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        // Настройка Kafka producer
        var props = new java.util.Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    private static class BinanceWebSocketListener implements WebSocket.Listener {
        private final RedisCommands<String, String> redisCommands;
        private final KafkaProducer<String, String> kafkaProducer;

        public BinanceWebSocketListener(RedisCommands<String, String> redisCommands, KafkaProducer<String, String> kafkaProducer) {
            this.redisCommands = redisCommands;
            this.kafkaProducer = kafkaProducer;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            System.out.println("WebSocket connection opened.");
            webSocket.request(1); // Запрашиваем первое сообщение
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            System.out.println("Message received: " + data);

            // Асинхронная обработка сообщения
            return CompletableFuture.runAsync(() -> {
                processMessage(data.toString());
            }).thenRun(() -> webSocket.request(1)); // Запрашиваем следующее сообщение
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            System.err.println("WebSocket error: " + error.getMessage());
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            System.out.println("WebSocket closed: " + reason + " (Code: " + statusCode + ")");
            return null;
        }

        private void processMessage(String message) {
            try {
                // Парсим JSON-данные
                JSONObject data = new JSONObject(message);
                if (data.has("k")) {
                    JSONObject klineData = data.getJSONObject("k");
                    String symbol = klineData.getString("s");
                    long eventTime = klineData.getLong("t");
                    String openPrice = klineData.getString("o");
                    String closePrice = klineData.getString("c");

                    String uniqueId = symbol + "_" + eventTime;

                    // Проверяем наличие уникального ID в Redis
                    if (redisCommands.exists(uniqueId) == 0) {
                        // Формируем сообщение для Kafka
                        JSONObject kafkaMessage = new JSONObject();
                        kafkaMessage.put("symbol", symbol);
                        kafkaMessage.put("event_time", eventTime);
                        kafkaMessage.put("open", openPrice);
                        kafkaMessage.put("close", closePrice);

                        // Отправляем сообщение в Kafka
                        String kafkaMessageString = kafkaMessage.toString();
                        kafkaProducer.send(new ProducerRecord<>("crypto_kline_data", symbol, kafkaMessageString));

                        System.out.println(kafkaMessageString);

                        // Сохраняем уникальный ID в Redis с TTL 30 минут
                        redisCommands.setex(uniqueId, 1800, "1");
                    } else {
                        System.out.println("Duplicate message detected, skipping: " + uniqueId);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
            }
        }
    }
}
