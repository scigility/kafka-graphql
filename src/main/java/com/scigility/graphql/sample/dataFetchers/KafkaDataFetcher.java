package com.scigility.graphql.sample.dataFetchers;

import com.scigility.graphql.sample.domain.Role;
import com.scigility.graphql.sample.domain.Kafka;
import com.scigility.graphql.sample.domain.Topic;

import java.util.Properties;
import java.util.*;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import org.apache.zookeeper.ZooKeeper;

import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.merapar.graphql.base.TypedValueMap;
import java.net.ConnectException;
import org.apache.zookeeper.KeeperException;
import lombok.val;

@Component
public class KafkaDataFetcher {

    private Log log = LogFactory.getLog(KafkaDataFetcher.class);

    //zookeeper
    //zkserver start
    //kafka
    //kafka-server-start.sh /usr/local/etc/kafka/server.properties
    public Kafka getKafkasByFilter(TypedValueMap arguments) {
      log.info("getKafkasByFilter");

      val kafka = Kafka.getInstance();

      return kafka;
    }

    public Kafka addKafkaTopicField(TypedValueMap arguments) {
        log.info("addKafkaTopicField");

        val kafka = Kafka.getInstance();

        val topic = new Topic();

        topic.setName(arguments.get("topic_name"));

        log.info(topic);

        createTopic(kafka,topic);
        
        return kafka;
    }

    public Kafka addKafkaTopicMessageField(TypedValueMap arguments) {
        log.info("addKafkaTopicMessageField");
        val kafka = Kafka.getInstance();
        log.info(kafka);

        //createTopic(kafka.getZookeeper(),topic.getName());

        //kafka.addToTopics(topic);

        return kafka;
    }

    public Kafka updateKafka(TypedValueMap arguments) {
        log.info("updateKafka");
        val kafka = Kafka.getInstance();

        if (arguments.containsKey("broker")) {
            kafka.setBroker(arguments.get("broker"));
        }

        if (arguments.containsKey("zookeeper")) {
            kafka.setZookeeper(arguments.get("zookeeper"));
        }
        return kafka;
    }

    public List<Topic> getTopics(Kafka kafka) {
        log.info("getTopics");
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

          log.info("List of Topics");
          int index = 0;
          for (String topicName : _topics) {
              log.info(topicName);
              val topic = new Topic();
              topic.setName(topicName);
              topics.add(topic);
          }
          
        } catch (KeeperException ex) {
            log.error("getTopics:KeeperException");
            ex.printStackTrace();
        } catch (ConnectException ex) {
            log.error("getTopics:ConnectException");
            ex.printStackTrace();
        } catch (NullPointerException ex) {
            log.error("getTopics:NullPointerException");
            ex.printStackTrace();
        } catch (Exception ex) {
            log.error("getTopics:Exception");
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }

        return topics;
    }
    //String zookeeperHosts,String topicName
    //kafka.getZookeeper(),topic.getName()
    private void createTopic(Kafka kafka, Topic topic){ // If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";
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
        log.info("topicName="+topic.getName());

        //String topicName = "testTopic";
        int noOfPartitions = 1;
        int noOfReplication = 1;
        Properties topicConfiguration = new Properties();
        log.info("AdminUtils.createTopic");
        //required: kafka.utils.ZkUtils,java.lang.String,int,int,java.util.Properties,kafka.admin.RackAwareMode
        AdminUtils.createTopic(zkUtils,
          topic.getName(), noOfPartitions, noOfReplication, topicConfiguration,
          RackAwareMode.Enforced$.MODULE$);

        try{
          TimeUnit.MILLISECONDS.sleep((long)(sessionTimeOutInMs*0.1));
        } catch (java.lang.InterruptedException e){}

        // Runtime rt = Runtime.getRuntime();
        // Process pr = rt.exec("kafka-topics --create --zookeeper "+ zookeeperHosts +
        //   " --replication-factor "+noOfReplication+
        //   " --partitions "+noOfPartitions+
        //   " --topic "+topicName
        //   );
      } catch (Exception ex) {
        log.error("getTopics");
        ex.printStackTrace();
      } finally {
        if (zkClient != null) {
            zkClient.close();
        }
      }
    }
}
