package com.scigility.graphql.sample.dataFetchers;

import com.scigility.graphql.sample.domain.Topic;
import com.scigility.graphql.sample.domain.TopicRecord;
import com.scigility.graphql.sample.domain.Kafka;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

import com.merapar.graphql.base.TypedValueMap;

import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.Arrays;
import java.util.LinkedHashMap;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import org.apache.zookeeper.ZooKeeper;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
@Component
public class DocumentFetcher {
    private Log log = LogFactory.getLog(TopicDataFetcher.class);

    public List<Topic> getDocumentByFilter(TypedValueMap arguments) {
        val kafka = Kafka.getInstance();

        log.info("getTopicsByFilter");

        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        int sessionTimeOutInMs = 20 * 1000; // 15 secs
        int connectionTimeOutInMs = 20 * 1000; // 10 secs
        boolean isSecureKafkaCluster = false;

        List<Topic> topics = new ArrayList<>();
        try {
            ZooKeeper zk = new ZooKeeper(
                    kafka.getZookeeper(), sessionTimeOutInMs, null);

            List<String> _topics = zk.getChildren(
                    "/brokers/topics", false);

            try{
                TimeUnit.MILLISECONDS.sleep((long)(sessionTimeOutInMs*0.1));
            } catch (java.lang.InterruptedException e){}

            log.info("List of Topics");
            int index = 0;
            for (String topicName : _topics) {
                log.info(topicName);
                val topic = new Topic();
                topic.setName(topicName);
                topics.add(topic);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }

        return topics;
    }

    public Topic addDocument(TypedValueMap arguments) {
        log.info("produceTopicRecord");
        log.info(arguments);
        val kafka = Kafka.getInstance();

        String name = arguments.get("name");
        String message = arguments.get("message");
        log.info(arguments.get("schema"));
        LinkedHashMap schema = arguments.get("schema");

        log.info("name:"+name+",message:"+message+",schema.type:"+schema.get("type"));
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBroker());
        props.put("acks", "all");
        props.put("retries", 2);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(props);
            producer.send(new ProducerRecord<String, String>(name, "key",message));
        } catch (Exception ex) {
            log.error("produceTopicRecord:Exception");
            ex.printStackTrace();
        } finally {
            if (producer != null) {
                producer.close();
            }
        }

        return null;
    }

    public List<TopicRecord> updateDocument(TypedValueMap arguments) {
        log.info("consumeTopicRecord");
        log.info(arguments);
        val kafka = Kafka.getInstance();

        String name = arguments.get("name");
        LinkedHashMap schema = arguments.get("schema");
        //String schemaType = schema.get("type");

        List<TopicRecord> topicRecords = new ArrayList<>();
        //String message = arguments.get("message");

        //log.info("name:"+name+",schema.type:"+schemaType);
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBroker());
        props.put("zookeeper.connect", kafka.getZookeeper());
        props.put("session.timeout.ms", "30000");
        props.put("group.id", name);
        props.put("acks", "all");
        props.put("retries", 5);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //consumer.subscribe(Arrays.asList("foo", "bar"));
        consumer.subscribe(Collections.singletonList(name));
        ConsumerRecords<String, String> records = consumer.poll(500);
        for (ConsumerRecord<String, String> record : records){
            log.info( "offset = " + record.offset() +
                    ", key = " + record.key() +
                    ", value = " + record.value() );

            TopicRecord topicRecord = new TopicRecord(
                    record.key(), record.value(), record.offset(), record.partition()
            );
            topicRecords.add(topicRecord);
        }
        //consumer.commitSync();
        return topicRecords;
    }

    public Topic deleteDocument(TypedValueMap arguments) {
        log.info("deleteDocument");

        return null;
    }
}
