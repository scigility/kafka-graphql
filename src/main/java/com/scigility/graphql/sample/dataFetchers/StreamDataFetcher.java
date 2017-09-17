package com.scigility.graphql.sample.dataFetchers;

import com.merapar.graphql.base.TypedValueMap;
import com.scigility.graphql.sample.domain.Kafka;
import com.scigility.graphql.sample.domain.TableRecord;
import com.scigility.graphql.sample.domain.Topic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class StreamDataFetcher {
    private Log log = LogFactory.getLog(StreamDataFetcher.class);

    public Map<Integer, Topic> table = new HashMap<>();

    private KafkaStreams streams;

    public List<TableRecord> getStreamByFilter(TypedValueMap arguments) {
        log.info("getStreamByFilter");
        Kafka kafka = Kafka.getInstance();

        String key = arguments.get("key");
        log.info("{"+key+"}");

        ReadOnlyKeyValueStore<String,Long> view = streams.store(
                "Counts", QueryableStoreTypes.<String, Long>keyValueStore());

        KeyValueIterator<String, Long> range = view.all();

        List<TableRecord> tableRecords = new ArrayList<>();
        while (range.hasNext()) {
            KeyValue<String, Long> next = range.next();
            log.info("table{" + next.key + ":" + next.value+"}");
            if( key == null || next.key.equals(key)) {
                TableRecord record = new TableRecord();
                record.setKey(next.key);
                record.setValue(next.value);
                tableRecords.add(record);
            }
        }

        return tableRecords;
    }

    public List<TableRecord> streamStart(TypedValueMap arguments) {
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
        config.put(StreamsConfig.STATE_DIR_CONFIG,"~/streams-pipe");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> textLines = builder.stream(topicIn);

        KTable<String, Long> wordCounts = textLines.
                mapValues(
                        textLine -> textLine.toLowerCase())
                .peek((key, value) -> log.info("mapValues:{"+key+":"+value+"}"))
                .flatMapValues(
                        lowerCasedTextLine -> Arrays.asList(
                                lowerCasedTextLine.split(" ")))
                .peek((key, value) -> log.info("flatMapValues:{"+key+":"+value+"}"))
                .selectKey((ignoredKey, word) -> word)
                .peek((key, value) -> log.info("selectKey:{"+key+":"+value+"}"))
                .groupByKey()
                .count("Counts");

        textLines.peek((key, value) -> log.info("stream:{:"+key+":"+value+"}"));

        wordCounts.filter((key, value) -> {log.info("table:{"+key+":"+value+"}");return true;});
        wordCounts.to(Serdes.String(), Serdes.Long(), topicOut);

        streams = new KafkaStreams(builder, config);

        //print topology
        log.info(streams.toString());

        //get queryableStoreName
        String queryableStoreName = wordCounts.queryableStoreName(); // returns null if KTable is not queryable

        log.info(queryableStoreName);

//        KTable<String, Long> query = builder.table(
//                TopologyBuilder.AutoOffsetReset.EARLIEST,
//                Serdes.String(),
//                Serdes.Long(),
//                topicOut
//        );
//
//        query.filter((key, value) -> {log.info("table:{"+key+":"+value+"}");return true;});
//        query.mapValues((value) -> log.info("query.table:key:"+key+":value:"+value));

        streams.start();

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ReadOnlyKeyValueStore<String,Long> view = streams.store(
                "Counts", QueryableStoreTypes.<String, Long>keyValueStore());

        KeyValueIterator<String, Long> range = view.all();

        List<TableRecord> tableRecords = new ArrayList<>();
        while (range.hasNext()) {
            KeyValue<String, Long> next = range.next();
            log.info("table{" + next.key + ":" + next.value+"}");
            TableRecord record = new TableRecord();
            record.setKey(next.key);
            record.setValue(next.value);

            tableRecords.add(record);
        }


        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return tableRecords;
    }

    public Topic streamStop(TypedValueMap arguments) {
        log.info("streamStop");
        Kafka kafka = Kafka.getInstance();

        return null;
    }
}
