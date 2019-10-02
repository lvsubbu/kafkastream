package com.subbu;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KafkaStreamApp {

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "trade_app");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, JsonNode> tradeInput = builder.stream(Serdes.String(), jsonSerde, "trade_topic");

        KTable<String, String> tradeOutput = tradeInput
                // create date as key
                .selectKey((ignore, value) -> dateAsKey(value))
                // extract totaltadval as value
                .mapValues(values -> totTrdVAL(values))
                // group based on key
                .groupByKey()
                // aggregate the group by tacking the max value
                .reduce((maxVal, val) -> retainMaxVal(maxVal, val));

        tradeOutput.to("trade_output");

        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String dateAsKey(JsonNode value) {
        return value.get("TIMESTAMP").asText();
    }

    private static String totTrdVAL(JsonNode value) {
        return value.get("TOTTRDVAL").asText();
    }
    private static String retainMaxVal(String maxVal, String val) {
        if (Double.valueOf(val) > Double.valueOf(maxVal)) {
            return val;
        }
        return maxVal;
    }
}
