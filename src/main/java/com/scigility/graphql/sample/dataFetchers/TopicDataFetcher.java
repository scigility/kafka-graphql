package com.scigility.graphql.sample.dataFetchers;

import com.scigility.graphql.sample.domain.TableRecord;
import com.scigility.graphql.sample.domain.Topic;
import com.scigility.graphql.sample.domain.TopicRecord;
import com.scigility.graphql.sample.domain.Kafka;
import org.apache.avro.Schema;
import lombok.val;

import com.merapar.graphql.base.TypedValueMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import java.util.*;
import java.util.concurrent.TimeUnit;

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

@Component
public class TopicDataFetcher {
    private Log log = LogFactory.getLog(TopicDataFetcher.class);

    public Map<String,Topic> topics = new HashMap<>();

    private KafkaStreams streams;

    public List<Topic> getTopicsByFilter(TypedValueMap arguments) {
        val kafka = Kafka.getInstance();

        log.info("getTopicsByFilter");

        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        int sessionTimeOutInMs = 20 * 1000; // 15 secs
        int connectionTimeOutInMs = 20 * 1000; // 10 secs
        boolean isSecureKafkaCluster = false;

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
                log.info("topicName:"+topicName);

                if ( !topics.containsKey(topicName) ) {
                    val topic = new Topic();
                    topic.setName(topicName);
                    topics.put(topicName,topic);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }

        return new ArrayList<Topic>(topics.values());
    }

    public String addTopic(TypedValueMap arguments) {
        log.info("addTopic");
        String result = "";
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        val kafka = Kafka.getInstance();

        LinkedHashMap schema = arguments.get("schema");
        log.info("schema:"+schema.toString());

        String topicName = arguments.get("name");
        log.info("topicName="+topicName);
        int noOfPartitions = 1;
        int noOfReplication = 1;

        try {
            log.info("zookeeperHosts="+kafka.getZookeeper());
            zkClient = new ZkClient(
                    kafka.getZookeeper(),
                    20 * 1000,
                    20 * 1000,
                    ZKStringSerializer$.MODULE$);

            zkUtils = new ZkUtils(zkClient, new ZkConnection(
                    kafka.getZookeeper()), false);

            Properties topicConfiguration = new Properties();

            log.info("AdminUtils.createTopic");
            AdminUtils.createTopic(zkUtils,
                    topicName, noOfPartitions, noOfReplication, topicConfiguration,
                    RackAwareMode.Safe$.MODULE$);

            log.info("AdminUtils.wait the result");
            try{
                TimeUnit.MILLISECONDS.sleep((long)(1000));
            } catch (java.lang.InterruptedException e){}

        } catch (Exception ex) {
            log.error("error:"+ex.getMessage());
            result = ex.getMessage();
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
            if ( !topics.containsKey(topicName) ) {
                val topic = new Topic();
                topic.setName(topicName);
                topic.setSchema(readSchema(schema));
                topics.put(topicName,topic);
                result = "topic "+topicName+" created";
            } else {
                val topic = topics.get(topicName);
                topic.setSchema(readSchema(schema));
                topics.put(topicName,topic);
                result = "topic "+topicName+" updated";
            }
        }
        return result;
    }

    public String produceTopicRecord(TypedValueMap arguments) {
        log.info("produceTopicRecord");
        String result = "";
        val kafka = Kafka.getInstance();

        String name = arguments.get("name");
        log.info("name:"+name);
        val topic = topics.get(name);

        LinkedHashMap record = arguments.get("record");
        String customer = (String)record.get("customer");
        Integer income = (Integer)record.get("income");
        Integer expenses = (Integer)record.get("expenses");
        log.info("constumer:"+customer+",income:"+income+",expenses:"+expenses);

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
            Schema schemaParsed = new Schema.Parser().parse(topic.getSchema());

            GenericData.Record avroRecord = new GenericData.Record(schemaParsed);
            avroRecord.put("customer",customer);
            avroRecord.put("income",income);
            avroRecord.put("expenses",expenses);

            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schemaParsed);
            producer.send(
                    new ProducerRecord<String, byte[]>(
                            name,
                            customer,
                            recordInjection.apply(avroRecord)
                    )
            );
            result = "record sent.";

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

    public List<TopicRecord> consumeTopicRecord(TypedValueMap arguments) {
        log.info("consumeTopicRecord");
        List<TopicRecord> topicRecords = new ArrayList<>();
        val kafka = Kafka.getInstance();

        log.info("arguments"+arguments.toString());

        String name = arguments.get("name");
        log.info("name:"+name);
        val topic = topics.get(name);

        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//latest, earliest, none
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBroker());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(name));

        ConsumerRecords<String, byte[]> records = consumer.poll(500);
        for (ConsumerRecord<String, byte[]> record : records){
            log.info( "offset = " + record.offset() +
                    ", key = " + record.key() +
                    ", value = " + record.value() );

            GenericRecord recordConverted = parseBytes(topic.getSchema(),record.value());

            log.info("{customer:"+recordConverted.get("customer")
                    + ",income:" + recordConverted.get("income")
                    + ",expenses:" + recordConverted.get("expenses")+"}");

            TopicRecord topicRecord = new TopicRecord(
                    record.key(), recordConverted.toString(), record.offset(), record.partition()
            );
            topicRecords.add(topicRecord);
        }
        //consumer.commitSync();
        return topicRecords;
    }

    public List<TableRecord> getStreamByFilter(TypedValueMap arguments) {
        log.info("getStreamByFilter");
        Kafka kafka = Kafka.getInstance();

        String storeKey = arguments.get("storeKey");
        log.info("{storeKey:"+storeKey+"}");

        Topic topic = topics.get(arguments.get("topic"));
        log.info("{topic.name:"+topic.getName()+":topci.schema:"+topic.getSchema()+"}");

        String key = arguments.get("key");
        log.info("{key:"+key+"}");

        ReadOnlyKeyValueStore<String,byte[]> view = streams.store(
                storeKey, QueryableStoreTypes.<String, byte[]>keyValueStore());

        KeyValueIterator<String, byte[]> range = view.all();

        List<TableRecord> tableRecords = new ArrayList<>();
        while (range.hasNext()) {
            KeyValue<String, byte[]> next = range.next();
            log.info("table{" + next.key + ":" + next.value+"}");
            if( key == null || next.key.equals(key)) {
                TableRecord record = new TableRecord();
                record.setKey(next.key);
                record.setValue(parseBytes(topic.getSchema(),next.value).toString());
                tableRecords.add(record);
            }
        }
        return tableRecords;
    }

    public List<TableRecord> streamStart(TypedValueMap arguments) {
        log.info("streamStart");
        Kafka kafka = Kafka.getInstance();
        val topicIn = topics.get(arguments.get("in"));
        val topicOut = topics.get(arguments.get("out"));
        val storeKey = (String)arguments.get("storeKey");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "contract-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBroker());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//latest, earliest, none
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.ByteArray().getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG,"streams-pipe");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, byte[]> constractStream = builder.stream(topicIn.getName());


        //.peek((key, record) -> log.info("mapValues:{"+key+":"+record.toString()+"}"))
        log.info("configuring stream");
        KTable<String, byte[]> wordCounts = constractStream
                .map((key, record) -> {
                    GenericRecord ParsedRecord = parseBytes(topicIn.getSchema(),record);
                    GenericRecord newParsedRecord = parseSchema(topicOut.getSchema());
                    Schema schemaParsed = new Schema.Parser().parse(topicOut.getSchema());
                    newParsedRecord.put("customer",ParsedRecord.get("customer"));
                    newParsedRecord.put("income",ParsedRecord.get("income"));
                    newParsedRecord.put("expenses",ParsedRecord.get("expenses"));
                    newParsedRecord.put("total",
                            (Integer)ParsedRecord.get("income")-
                                    (Integer)ParsedRecord.get("expenses"));

                    Injection<GenericRecord, byte[]> recordInjection =
                            GenericAvroCodecs.toBinary(schemaParsed);
                    log.info(ParsedRecord.toString());
                    log.info(newParsedRecord.toString());
                    return KeyValue.pair(key,recordInjection.apply(newParsedRecord));
                })
                .groupByKey()
                .reduce((aggValue, newValue) -> {
                    GenericRecord ParsedRecord = parseBytes(topicOut.getSchema(),aggValue);
                    GenericRecord ParsedNewRecord = parseBytes(topicOut.getSchema(),newValue);
                    Schema schemaParsed = new Schema.Parser().parse(topicOut.getSchema());
                    ParsedRecord.put("income",
                            (Integer)ParsedRecord.get("income")+
                                (Integer)ParsedNewRecord.get("income"));

                    ParsedRecord.put("expenses",
                            (Integer)ParsedRecord.get("expenses")+
                                    (Integer)ParsedNewRecord.get("expenses"));

                    ParsedRecord.put("total",
                            (Integer)ParsedRecord.get("total")+
                                    (Integer)ParsedNewRecord.get("total"));
                    log.info(ParsedRecord.toString());
                    log.info(ParsedNewRecord.toString());
                    Injection<GenericRecord, byte[]> recordInjection =
                            GenericAvroCodecs.toBinary(schemaParsed);
                    return recordInjection.apply(ParsedRecord);
                },storeKey);

        constractStream.peek((key, value) -> {
            GenericRecord ParsedRecord = parseBytes(topicIn.getSchema(),value);
            log.info("stream:{:"+key+":"+ParsedRecord.toString()+"}");
        });

        wordCounts.filter((key, value) -> {
            GenericRecord ParsedRecord = parseBytes(topicOut.getSchema(),value);
            log.info("table:{"+key+":"+ParsedRecord.toString()+"}");
            return true;
        });

        wordCounts.to(Serdes.String(), Serdes.ByteArray(), topicOut.getName());

        streams = new KafkaStreams(builder, config);

        //print topology
        log.info("printing stream topology");
        log.info(streams.toString());

        //get queryableStoreName
        String queryableStoreName = wordCounts.queryableStoreName(); // returns null if KTable is not queryable

        log.info("stream Store Name");
        log.info(queryableStoreName);

        log.info("Starting Stream");
        streams.start();

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        log.info("Reading Stream Value");
        ReadOnlyKeyValueStore<String,byte[]> view = streams.store(
                storeKey, QueryableStoreTypes.<String, byte[]>keyValueStore()
        );

        KeyValueIterator<String, byte[]> range = view.all();

        List<TableRecord> tableRecords = new ArrayList<>();
        while (range.hasNext()) {
            KeyValue<String, byte[]> next = range.next();
            log.info("table{" + next.key + ":" + next.value+"}");
            TableRecord record = new TableRecord();
            record.setKey(next.key);
            record.setValue(parseBytes(topicOut.getSchema(),next.value).toString());

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

    private GenericRecord parseSchema(String schema){
        Schema schemaParsed = new Schema.Parser().parse(schema);
        GenericData.Record avroRecord = new GenericData.Record(schemaParsed);
        return avroRecord;
    }

    private GenericRecord parseBytes(String schema, byte[] data){
        Schema schemaParsed = new Schema.Parser().parse(schema);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schemaParsed);
        GenericRecord recordConverted = recordInjection.invert(data).get();
        return recordConverted;
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

}
