package ru.highloadjava.coursework;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import ru.highloadjava.coursework.datamodel.CryptoAggregatedData;
import ru.highloadjava.coursework.datamodel.CryptoData;
import ru.highloadjava.coursework.deserialization.KafkaCryptoDataSchema;
import ru.highloadjava.coursework.processFunction.AverageAggregatedFunction;
import ru.highloadjava.coursework.serialization.CryptoAggregatedDataSerializationSchema;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class FlinkKafkaJob {
    /*public FlinkKafkaJob() {
    }*/

    public void execute() throws Exception {
        // Конфигурация Kafka
        String KAFKA_SERVER = "localhost:9092";
        String KAFKA_INPUT_TOPIC = "crypto_kline_data";
        String KAFKA_OUTPUT_TOPIC = "crypto_data_aggregated";

        // Настройка Flink окружения
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<CryptoData> kafkaSource = KafkaSource.<CryptoData>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setTopics(KAFKA_INPUT_TOPIC)
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                //.setValueOnlyDeserializer(new JsonDeserializationSchema<>(CryptoData.class))
                .setValueOnlyDeserializer(new KafkaCryptoDataSchema())
                .build();

        //setParallelism необходимо установить на number kafka topic partitions,
        // иначе водяной знак не будет работать для источника, и данные не будут переданы в приемник.
        //forBoundedOutOfOrderness ниже работает на основе времени обработки/приема kafka,
        // а не времени события. Поэтому необходимо иметь отдельную стратегию водяных знаков, если вам нужно обрабатывать на основе времени события
        DataStream<CryptoData> stream = env.fromSource(kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)), "Kafka prices source").setParallelism(1);


        SerializableTimestampAssigner<CryptoData> sz = new SerializableTimestampAssigner<CryptoData>() {
            @Override
            public long extractTimestamp(CryptoData element, long recordTimestamp) {
                try {
                    return element.getEvent_time();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        WatermarkStrategy<CryptoData> watermarkStrategy =
                WatermarkStrategy.<CryptoData>forBoundedOutOfOrderness(Duration.ofMillis(100)).withTimestampAssigner(sz);


        DataStream<CryptoData> watermarkDataStream = stream.assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<CryptoAggregatedData> groupedData = watermarkDataStream.keyBy(CryptoData::getSymbol)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(3000), Duration.ofMillis(500)))
                .apply(new AverageAggregatedFunction());


        CryptoAggregatedDataSerializationSchema serializer = new CryptoAggregatedDataSerializationSchema(KAFKA_OUTPUT_TOPIC);

        // Конфигурация Kafka Sink (Producer) для записи в новый топик
        KafkaSink<CryptoAggregatedData> kafkaSink = KafkaSink
                .<CryptoAggregatedData>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        groupedData
                .sinkTo(kafkaSink)
                .name("kafka_sink_aggregated");


        // Запуск Flink приложения
        env.execute("Crypto Data Stream Processing");
    }

    public static void main(String[] args) throws Exception {
        FlinkKafkaJob job = new FlinkKafkaJob();
        job.execute();
    }
}
