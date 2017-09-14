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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.ZooKeeper;
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

        return null;
    }

    public Topic streamStop(TypedValueMap arguments) {
        log.info("streamStop");
        Kafka kafka = Kafka.getInstance();

        return null;
    }
}
