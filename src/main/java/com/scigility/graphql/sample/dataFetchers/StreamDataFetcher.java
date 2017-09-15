package com.scigility.graphql.sample.dataFetchers;

import com.merapar.graphql.base.TypedValueMap;
import com.scigility.graphql.sample.domain.Kafka;
import com.scigility.graphql.sample.domain.Topic;
import com.scigility.graphql.sample.domain.TopicRecord;
import kafka.utils.ZkUtils;
import lombok.val;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
public class StreamDataFetcher {
    private Log log = LogFactory.getLog(StreamDataFetcher.class);

    public Map<Integer, Topic> table = new HashMap<>();

    public List<Topic> getStreamByFilter(TypedValueMap arguments) {
        log.info("getStreamByFilter");
        Kafka kafka = Kafka.getInstance();



        return null;
    }

    public Topic streamStart(TypedValueMap arguments) {
        log.info("streamStart");
        Kafka kafka = Kafka.getInstance();
        String topicIn = arguments.get("in");
        String topicOut = arguments.get("out");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBroker());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//latest, earliest, none
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> textLines = builder.stream(topicIn);

        KTable<String, Long> wordCounts = textLines.
                mapValues( textLine -> textLine.toLowerCase())
                .flatMapValues(
                        lowerCasedTextLine -> Arrays.asList(
                                lowerCasedTextLine.split(" "))
                )
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count("Counts");

        wordCounts.to(Serdes.String(), Serdes.Long(), topicOut);

        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return null;
    }

    public Topic streamStop(TypedValueMap arguments) {
        log.info("streamStop");
        Kafka kafka = Kafka.getInstance();

        return null;
    }
}
