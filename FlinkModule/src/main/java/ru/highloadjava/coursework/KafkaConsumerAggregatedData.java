package ru.highloadjava.coursework;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.highloadjava.coursework.datamodel.CryptoAggregatedData;
import ru.highloadjava.coursework.deserialization.CryptoAggregatedDataDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class KafkaConsumerAggregatedData {
    //private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerAggregatedData.class);

    public static void main(String[] args) {


        log.info("Kafka consumer!");
        String KAFKA_SERVER = "localhost:9092";
        String groupId = "aggregated-data-consumer";
        //String topics = "crypto_kline_data";
        String topics = "crypto_data_aggregated";


        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CryptoAggregatedDataDeserializer.class.getName());
        //properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CryptoDataDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, CryptoAggregatedData> consumer = new KafkaConsumer<>(properties);
        //KafkaConsumer<String, CryptoConsumerData> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topics));

        while(true) {
            ConsumerRecords<String, CryptoAggregatedData> records = consumer.poll(Duration.ofMillis(10000));
            //ConsumerRecords<String, CryptoConsumerData> records = consumer.poll(Duration.ofMillis(1000));


            for (ConsumerRecord<String, CryptoAggregatedData> record: records) {
                CryptoAggregatedData data = record.value();
                /*log.info("Key: " + record.key() +
                        " Symbol: " + data.getSymbol() +
                        " Average Price: " + data.getAverage_price() +
                        " Event Time: " + data.getEvent_time() +
                        " Partition: " + record.partition() +
                        " Offset: " + record.offset());*/
                log.info("Key: {}, Symbol: {}, Average Price: {}, Event Time: {}, Partition: {}, Offset: {}",
                        record.key(),
                        data.getSymbol(),
                        data.getAverage_price(),
                        data.getEvent_time(),
                        record.partition(),
                        record.offset());
            }
        }
    }
}
