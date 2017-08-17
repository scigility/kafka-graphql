package com.scigility.graphql.sample.dataFetchers;

import com.merapar.graphql.base.TypedValueMap;
import com.scigility.graphql.sample.domain.Role;
import com.scigility.graphql.sample.domain.Kafka;
import com.scigility.graphql.sample.domain.Topic;
import lombok.val;
import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

@Component
public class KafkaDataFetcher {
    private Log log = LogFactory.getLog(KafkaDataFetcher.class);
    public Map<Integer, Kafka> kafkas = new HashMap<>();

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
        //TODO GET TOPICS
        return new ArrayList<Topic>();
    }
}
