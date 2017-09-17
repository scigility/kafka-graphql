package com.scigility.graphql.sample.dataFetchers;


import com.scigility.graphql.sample.domain.Topic;
import com.scigility.graphql.sample.domain.TopicRecord;
import com.scigility.graphql.sample.domain.Kafka;
import org.apache.avro.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

import com.merapar.graphql.base.TypedValueMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import java.util.*;
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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;


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
public class TopicDataFetcher {
    private Log log = LogFactory.getLog(TopicDataFetcher.class);

    public Map<Integer, Topic> topics = new HashMap<>();

    public List<Topic> getTopicsByFilter(TypedValueMap arguments) {
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

    public String addTopic(TypedValueMap arguments) {
        log.info("addTopic");

        val kafka = Kafka.getInstance();

        String name = arguments.get("name");
        String result = "";

        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        int sessionTimeOutInMs = 20 * 1000; // 15 secs
        int connectionTimeOutInMs = 20 * 1000; // 10 secs
        boolean isSecureKafkaCluster = false;

        try {
            log.info("zookeeperHosts="+kafka.getZookeeper());
            zkClient = new ZkClient(
                    kafka.getZookeeper(),
                    sessionTimeOutInMs,
                    connectionTimeOutInMs,
                    ZKStringSerializer$.MODULE$);

            // Security for Kafka was added in Kafka 0.9.0.0

            zkUtils = new ZkUtils(zkClient, new ZkConnection(
                    kafka.getZookeeper()), isSecureKafkaCluster);
            log.info("topicName="+name);

            //String topicName = "testTopic";
            int noOfPartitions = 1;
            int noOfReplication = 1;
            Properties topicConfiguration = new Properties();
            log.info("AdminUtils.createTopic");
            //required: kafka.utils.ZkUtils,java.lang.String,int,int,java.util.Properties,kafka.admin.RackAwareMode
            AdminUtils.createTopic(zkUtils,
                    name, noOfPartitions, noOfReplication, topicConfiguration,
                    RackAwareMode.Enforced$.MODULE$);

            try{
                TimeUnit.MILLISECONDS.sleep((long)(sessionTimeOutInMs*0.1));
            } catch (java.lang.InterruptedException e){}

            result = "topic created";
        } catch (Exception ex) {
            log.error("getTopics:"+ex.getMessage());
            result = ex.getMessage();
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        return result;
    }

    public String produceTopicRecord(TypedValueMap arguments) {
        log.info("produceTopicRecord");
        log.info(arguments);
        val kafka = Kafka.getInstance();

        String result = "";
        String name = arguments.get("name");
        log.info("name:"+name);

        LinkedHashMap record = arguments.get("record");
        String customer = (String)record.get("customer");
        Integer income = (Integer)record.get("income");
        Integer expenses = (Integer)record.get("expenses");
        log.info("constumer:"+customer+",income:"+income+",expenses:"+expenses);

        LinkedHashMap schema = arguments.get("schema");
        log.info("schema:"+schema.toString());

        //log.info(arguments.get("schema"));
        //LinkedHashMap schema = arguments.get("schema");

        //log.info("name:"+name+",message:"+message+",schema.type:"+schema.get("type"));

        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBroker());
        props.put("acks", "all");
        props.put("retries", 2);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<String, byte[]> producer = null;
        try {
            producer = new KafkaProducer<>(props);
            Schema.Parser parser = new Schema.Parser();
            Schema schemaParsed = parser.parse(readSchema(schema));

            GenericData.Record avroRecord = new GenericData.Record(schemaParsed);
            avroRecord.put("customer",customer);
            avroRecord.put("income",income);
            avroRecord.put("expenses",expenses);

            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schemaParsed);
            producer.send(
                    new ProducerRecord<String, byte[]>(
                            name,
                            "key",
                            recordInjection.apply(avroRecord)
                    )
            );
            result = "rescord sent.";

            Thread.sleep(250);
        } catch (Exception ex) {
            result =  ex.getMessage();
            log.error("produceTopicRecord:Exception");
            ex.printStackTrace();
        } finally {
            if (producer != null) {
                producer.close();
            }
        }

        return result;
    }

    //{name=contract, type=record, fields=[{name=name, type=string}, {name=income, type=int}, {name=expenses, type=int}]}
    //  },{\"name\":\"str2\", \"type\":\"string\" },""  { \"name\":\"int1\", \"type\":\"int\" }""]}";
    private String readSchema(LinkedHashMap schema){
        final StringBuilder result = new StringBuilder();
        result.append("{");
        //{"\"type\":\"record\",
        result.append(doubleQuote("type",(String)schema.get("type"))+",");
        //"\"name\":\"myrecord\",
        result.append(doubleQuote("name",(String)schema.get("name"))+",");
        //"\"fields\":[
        result.append(doubleQuote("fields")+":[");
        ((List<LinkedHashMap>)schema.get("fields")).stream().forEach(
                field -> {
                    result.append("{");
                    //{ \"name\":\"str1\"
                    result.append(doubleQuote("name",(String)field.get("name")));
                    //, \"type\":\"string\" }
                    result.append(",");
                    result.append(doubleQuote("type",(String)field.get("type")));
                    result.append("},");
                }
        );
        // remove the last ","
        result.deleteCharAt(result.length() - 1);
        result.append("]}");
        log.info(result.toString());
        return result.toString();
    }
    private String doubleQuote(String field){
        return  "\""+field+"\"";
    }
    private String doubleQuote(String field, String value){
        return  "\""+field+"\":\""+value+"\"";
    }

    public List<TopicRecord> consumeTopicRecord(TypedValueMap arguments) {
        log.info("consumeTopicRecord");
        log.info(arguments);
        val kafka = Kafka.getInstance();

        String name = arguments.get("name");
        LinkedHashMap<String,String> schema = arguments.get("schema");
        String serdeKey = schema.get("serdeKey");
        String serdeValue = schema.get("serdeValue");

        List<TopicRecord> topicRecords = new ArrayList<>();
        //String message = arguments.get("message");
        log.info("name:"+name);
        log.info("serdeKey:"+serdeKey);
        log.info("serdeValue:"+serdeValue);
        //log.info("name:"+name+",schema.type:"+schemaType);

        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//latest, earliest, none
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBroker());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(name));

        ConsumerRecords<String, Long> records = consumer.poll(500);
        for (ConsumerRecord<String, Long> record : records){
            log.info( "offset = " + record.offset() +
                    ", key = " + record.key() +
                    ", value = " + record.value() );

            TopicRecord topicRecord = new TopicRecord(
                    record.key(), Long.toString(record.value()), record.offset(), record.partition()
            );
            topicRecords.add(topicRecord);
        }
        //consumer.commitSync();
        return topicRecords;
    }

    public Topic updateTopic(TypedValueMap arguments) {
        log.info("updateTopic");
        val topic = topics.get(arguments.get("id"));

        if (arguments.containsKey("name")) {
            topic.setName(arguments.get("name"));
        }

        return topic;
    }

    //TODO
    public Topic deleteTopic(TypedValueMap arguments) {
        log.info("deleteTopic");
        val topic = topics.get(arguments.get("id"));

        //topics.remove(topic.getId());

        return topic;
    }
}
