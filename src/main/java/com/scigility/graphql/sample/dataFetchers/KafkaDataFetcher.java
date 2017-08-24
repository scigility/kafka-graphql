package com.scigility.graphql.sample.dataFetchers;

import com.merapar.graphql.base.TypedValueMap;
import com.scigility.graphql.sample.domain.Role;
import com.scigility.graphql.sample.domain.Kafka;
import com.scigility.graphql.sample.domain.Topic;
import lombok.val;
import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import java.util.*;

@Component
public class KafkaDataFetcher {
    private Log log = LogFactory.getLog(KafkaDataFetcher.class);
    public Map<Integer, Kafka> kafkas = new HashMap<>();

    //zookeeper
    //zkserver start
    //kafka
    //kafka-server-start.sh /usr/local/etc/kafka/server.properties

    public List<Kafka> getKafkasByFilter(TypedValueMap arguments) {
      log.info("getKafkasByFilter");
        Integer id = arguments.get("id");

        if (id != null) {
            return Collections.singletonList(kafkas.get(id));
        } else {
            return new ArrayList<>(kafkas.values());
        }
    }

    public Kafka addKafka(TypedValueMap arguments) {
        log.info("addKafka");
        val kafka = new Kafka();
        kafka.setId(arguments.get("id"));
        kafka.setBroker(arguments.get("broker"));
        kafka.setZookeeper(arguments.get("zookeeper"));

        kafkas.put(kafka.getId(), kafka);

        return kafka;
    }

    public Kafka addKafkaTopicField(TypedValueMap arguments) {
        log.info("addKafkaTopicField");
        val kafka = kafkas.get(arguments.get("kafka_id"));
        log.info(kafka);
        val topic = new Topic();
        topic.setId(arguments.get("topic_id"));
        topic.setName(arguments.get("topic_name"));

        log.info(topic);

        createTopic(kafka.getZookeeper(),topic.getName());

        kafka.addToTopics(topic);

        return kafka;
    }

    public Kafka updateKafka(TypedValueMap arguments) {
        log.info("updateKafka");
        val kafka = kafkas.get(arguments.get("id"));

        if (arguments.containsKey("broker")) {
            kafka.setBroker(arguments.get("broker"));
        }

        if (arguments.containsKey("zookeeper")) {
            kafka.setZookeeper(arguments.get("zookeeper"));
        }
        return kafka;
    }

    public Kafka deleteKafka(TypedValueMap arguments) {
        log.info("deleteKafka");
        val kafka = kafkas.get(arguments.get("id"));

        kafkas.remove(kafka.getId());

        return kafka;
    }

    public List<Topic> getTopics(Kafka kafka) {
        log.info("getTopics");

        val topics = kafka.getTopics();
        return topics;
    }

    private void createTopic(String zookeeperHosts,String topicName){ // If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";
      ZkClient zkClient = null;
      ZkUtils zkUtils = null;
    //   String zookeeperConnect = "zkserver1:2181,zkserver2:2181";
    // int sessionTimeoutMs = 10 * 1000;
    // int connectionTimeoutMs = 8 * 1000;
    //
    // String topic = "my-topic";
    // int partitions = 2;
    // int replication = 3;
        try {
            log.info("zookeeperHosts="+zookeeperHosts);
            //String zookeeperHosts = "192.168.20.1:2181";
            int sessionTimeOutInMs = 15 * 1000; // 15 secs
            int connectionTimeOutInMs = 10 * 1000; // 10 secs

            //zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);

            zkClient = new ZkClient(
                    zookeeperHosts,
                    sessionTimeOutInMs,
                    connectionTimeOutInMs,
                    ZKStringSerializer$.MODULE$);

            // Security for Kafka was added in Kafka 0.9.0.0
            boolean isSecureKafkaCluster = false;

            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), isSecureKafkaCluster);
            log.info("topicName="+topicName);
            //String topicName = "testTopic";
            int noOfPartitions = 1;
            int noOfReplication = 1;
            Properties topicConfiguration = new Properties();
            log.info("AdminUtils.createTopic");
            //required: kafka.utils.ZkUtils,java.lang.String,int,int,java.util.Properties,kafka.admin.RackAwareMode
            AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration,null);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}
